package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	jsoniter "github.com/json-iterator/go"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

func timeMeasurement(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("[%s] execution time: %s", name, elapsed)
}

func readLines(file string) []logentry {
	r, err := os.Open(file)
	defer r.Close()
	if err != nil {
		log.Fatalf("Unable to open file %s, %v\n", file, err)
	}

	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)
	array := make([]logentry, 0, config.File.RotateInterval)

	for scanner.Scan() {
		text := scanner.Text()

		var le logentry
		_ = json.Unmarshal([]byte(text), &le)

		array = append(array, le)
		//log.Println("file: ", cap(array), len(array), le.Hash, le.Timestamp)
	}

	return array

}

func filesToBackfill(sess *session.Session) []*s3.Object {
	svc := s3.New(sess)
	resp, err := svc.ListObjectsV2(
		&s3.ListObjectsV2Input{
			Bucket: aws.String(config.Aws.Bucket),
			Prefix: &config.Aws.BucketFolder,
		})
	if err != nil {
		log.Println("Unable to list items in bucket %q, %v", config.Aws.Bucket, err)
	}

	return resp.Contents
}

func download(downloader *s3manager.Downloader, fileName string) []byte {
	defer timeMeasurement(time.Now(), "donwload")
	buf := aws.NewWriteAtBuffer([]byte{})

	_, err := downloader.Download(buf, //dest_file,
		&s3.GetObjectInput{
			Bucket: aws.String(config.Aws.Bucket),
			Key:    aws.String(fileName), //*item.Key),
		})
	if err != nil {
		log.Println("Unable to download item %q, %v", fileName, err)
	}

	//log.Println("buf.Len(): ", len(buf.Bytes()))
	//log.Println("Size:         ", *item.Size)

	return buf.Bytes()

}

func uncompress(dataCompressed []byte) bytes.Buffer {
	defer timeMeasurement(time.Now(), "uncompress")
	var buf bytes.Buffer

	gr, err := gzip.NewReader(bytes.NewBuffer(dataCompressed))
	defer gr.Close()
	dataCompressed, err = ioutil.ReadAll(gr)
	if err != nil {
		log.Println("ReadAll %v", err)
	}
	buf.Write(dataCompressed)

	return buf

}

func checkSHA256(data []byte, filePath string) error {
	defer timeMeasurement(time.Now(), "checkSHA256")
	sum := sha256.Sum256(data)
	checksumComputed := fmt.Sprintf("%x", sum)

	keySplit := strings.Split(filePath, ".")
	checksumOrg := keySplit[len(keySplit)-2]

	if checksumComputed == checksumOrg {
		return nil
	} else {
		errString := fmt.Sprintf("SHA256 FAILED. checksumComputed: %s, checksumOrg: %s", checksumComputed, checksumOrg)
		return errors.New(errString)
	}

}

func scanLines(reader io.Reader) []logentry {
	defer timeMeasurement(time.Now(), "scanLines")

	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

	array := make([]logentry, 0, config.File.RotateInterval)

	for scanner.Scan() {
		var le logentry

		le.RawJsonString = scanner.Text()
		le.Timestamp = time.Unix(0, jsoniter.Get(scanner.Bytes(), "started_at").ToInt64()*1000000)
		le.Hash = hash(scanner.Bytes())

		array = append(array, le)
	}

	return array
}

func processFile(filePath string, downloader *s3manager.Downloader) []logentry {
	defer timeMeasurement(time.Now(), "processFile")
	log.Println("Processing file ", filePath)

	dataCompressed := download(downloader, filePath)
	dataUncompressedBuffer := uncompress(dataCompressed)

	checkSHA256Err := checkSHA256(dataUncompressedBuffer.Bytes(), filePath)
	if checkSHA256Err != nil {
		log.Printf("checkSHA256 %v\n\tSkipping file: %s\n", checkSHA256Err, filePath)
		var rows []logentry
		return rows
	} else {
		log.Println("SHA256 checksum OK.")
	}

	rows := scanLines(&dataUncompressedBuffer)
	//log.Println("rows: ", cap(rows), len(rows), rows[0].Timestamp)
	return rows

}

func copyToStaging(rows []logentry, conn *pgxpool.Pool) {
	defer timeMeasurement(time.Now(), "copyToStaging")
	tempTable := config.Timescale.Table + "_backfill"
	cnt, err := conn.CopyFrom(
		context.Background(),
		pgx.Identifier{tempTable},
		config.Timescale.ColumnNames,
		pgx.CopyFromSlice(len(rows), func(i int) ([]interface{}, error) {
			return []interface{}{rows[i].Timestamp, rows[i].Hash, []byte(rows[i].RawJsonString)}, nil
		}),
	)

	if err != nil { // no need to panic here, if something is wrong with a DB other tnah context cancel, then we keep prosessing data
		log.Printf("Unable to copy %d rows to TimescaleDB, %v\n", len(rows), err)
	} else {
		log.Printf("Coppied %d rows to %s.\n", cnt, tempTable)
	}
}

func onNotify(c *pgconn.PgConn, n *pgconn.Notice) {
	log.Println("Timescale Message:", n.Message)
}

func callDecompressBackfill(conn *pgxpool.Pool) {
	defer timeMeasurement(time.Now(), "callDecompressBackfill")

	log.Println("callDecompressBackfill")

	tempTable := config.Timescale.Table + "_backfill"
	decompressBackfill := "CALL decompress_backfill(" +
		"staging_table=>'" + tempTable + "', " +
		"destination_hypertable=>'" + config.Timescale.Table + " ', " +
		"on_conflict_action=>'NOTHING',delete_from_staging=>true, cols=>'*')"

	_, err := conn.Exec(context.Background(), decompressBackfill)
	if err != nil {
		log.Printf("CALL decompress_backfill failed: %v\n", err)
	}

}

func processInterval(wg *sync.WaitGroup, conn *pgxpool.Pool) {
	defer timeMeasurement(time.Now(), "processInterval")
	wg.Wait()

	callDecompressBackfill(conn)
}

func backfill() { // AWS
	defer timeMeasurement(time.Now(), "backfill")
	/*
		- list s3 file by file
		- unzip
		- process line by line
		- insert to temp table
		- backfill
		- maintain a "done" list
	*/

	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(config.Aws.Region),
		Credentials: credentials.NewStaticCredentials(config.Aws.AccessKeyId, config.Aws.SecretAccessKey, ""),
	}))

	conf, err := pgxpool.ParseConfig(config.Timescale.ConnectionString)
	if err != nil {
		log.Fatalf("Unable to parse DB config: %v\n", err)
	}
	conf.ConnConfig.OnNotice = onNotify
	conn, err := pgxpool.ConnectConfig(context.Background(), conf)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}

	filesList := filesToBackfill(sess)

	downloader := s3manager.NewDownloader(sess)

	nFiles := len(filesList)
	//nFiles = 2
	filesParallelProcessing := 4
	var wg sync.WaitGroup
	for i := 1; i < nFiles+1; i++ {
		filePath := *filesList[i-1].Key

		wg.Add(1)
		go func() {
			defer wg.Done()
			rows := processFile(filePath, downloader)
			copyToStaging(rows, conn)
		}()

		if i%filesParallelProcessing == 0 {
			processInterval(&wg, conn)
		}

	}
	processInterval(&wg, conn)

}

func backfill_db() {

	//ctx := context.Background()

	//connectionString := fmt.Sprintf("user=%s password=%s host=%s sslmode=disable dbname=dh", "postgres", "password", "localhost")

	//conf, err := pgx.ParseConfig(connectionString)
	conf, err := pgxpool.ParseConfig(config.Timescale.ConnectionString)

	if err != nil {
		fmt.Println(err)
	}

	conf.ConnConfig.OnNotice = onNotify

	conn, err := pgxpool.ConnectConfig(context.Background(), conf) //config.Timescale.ConnectionString)
	//conn, err := pgx.ConnectConfig(ctx, conf)
	if err != nil {
		log.Fatalln(err)
	}

	//defer conn.Close(ctx)

	query := `DO language plpgsql $$
BEGIN
   RAISE NOTICE 'hello, world!';
END
$$;`
	//conn.Config().OnNotice = onNotify

	_, err = conn.Exec(context.Background(), query)

	if err != nil {
		log.Fatal(err)
	}

	//conn, err := pgxpool.Connect(context.Background(), config.Timescale.ConnectionString)
	//if err != nil {
	//	log.Fatalf("Unable to connect to database: %v\n", err)
	//}
	//
	//ctx := context.Background()
	//
	//tempTable := config.Timescale.Table + "_backfill"
	//createTable := "CREATE TABLE " + tempTable + " AS select time, log_entry_hash,log_entry from " + config.Timescale.Table + " WITH NO data"
	////createTable := "CREATE TABLE " + tempTable + " AS select * from "+ config.Timescale.Table + " WITH NO data"
	//fmt.Println(createTable)
	return

	//dropTable := "Drop TABLE " + tempTable
	//_, err = conn.Exec(context.Background(), dropTable)
	//if err != nil {
	//	fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
	//	os.Exit(1)
	//}
	return

}
