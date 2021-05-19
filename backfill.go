package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

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

func backfill() { // AWS
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

	svc := s3.New(sess)
	resp, err := svc.ListObjectsV2(
		&s3.ListObjectsV2Input{
			Bucket: aws.String(config.Aws.Bucket),
			Prefix: &config.Aws.BucketFolder,
		})
	if err != nil {
		log.Println("Unable to list items in bucket %q, %v", config.Aws.Bucket, err)
	}

	//dest_filename := "/home/martin/Downloads/s3_download_testing.gz"
	//
	//downloader := s3manager.NewDownloader(sess)
	////var buf bytes.Buffer
	//buf := aws.NewWriteAtBuffer([]byte{})
	for _, item := range resp.Contents {
		fmt.Println("Name:         ", *item.Key)

		downloader := s3manager.NewDownloader(sess)
		//var buf bytes.Buffer
		buf := aws.NewWriteAtBuffer([]byte{})

		//dest_file, err := os.Create(dest_filename)
		//if err != nil {
		//	log.Println("Unable to open file %q, %v", dest_filename, err)
		//}
		//
		//defer dest_file.Close()

		_, err := downloader.Download(buf, //dest_file,
			&s3.GetObjectInput{
				Bucket: aws.String(config.Aws.Bucket),
				Key:    aws.String(*item.Key),
			})
		if err != nil {
			log.Println("Unable to download item %q, %v", item, err)
		}

		log.Println("buf.Len(): ", len(buf.Bytes()))
		log.Println("Size:         ", *item.Size)

		var buf2 bytes.Buffer
		data := buf.Bytes()
		gr, err := gzip.NewReader(bytes.NewBuffer(data))
		defer gr.Close()
		data, err = ioutil.ReadAll(gr)
		if err != nil {
			log.Println("ReadAll %v", err)
		}
		buf2.Write(data)

		// check SHA256
		sum := sha256.Sum256(buf2.Bytes())
		checksumComputed := fmt.Sprintf("%x", sum)

		key_split := strings.Split(*item.Key, ".")
		checksumOrg := key_split[len(key_split)-2]

		if checksumComputed == checksumOrg {
			log.Printf("SHA256 checksum OK. %s\n", checksumComputed)
		} else {
			log.Printf("SHA256 FAILED. checksumComputed: %s, checksumOrg: %s\n", checksumComputed, checksumOrg)
		}

		/////

		scanner := bufio.NewScanner(&buf2)
		scanner.Split(bufio.ScanLines)

		array := make([]logentry, 0, config.File.RotateInterval)

		for scanner.Scan() {
			text := scanner.Text()

			var le logentry
			_ = json.Unmarshal([]byte(text), &le)

			array = append(array, le)
			log.Println("file: ", cap(array), len(array), le.Timestamp)
			break
		}

		//bufg := bytes.NewBuffer()
		//_, err = gzip.NewReader(bufg)
		//if err != nil {
		//	log.Fatal("NewReader %v ", err)
		//}

		//fmt.Printf("Name: %s\nComment: %s\nModTime: %s\n\n", zr.Name, zr.Comment, zr.ModTime.UTC())
		//fmt.Println("Downloaded", dest_filename, numBytes, "bytes")

		//fmt.Println("Last modified:", *item.LastModified)

		//fmt.Println("Storage class:", *item.StorageClass)
		//fmt.Println("")
		break
	}

	//uploader := s3manager.NewUploader(sess)
	//_, err := uploader.Upload(&s3manager.UploadInput{
	//	Bucket: aws.String(config.Aws.Bucket),
	//	Key:    aws.String(filenameDst),
	//	Body:   file,
	//})
	//if err != nil {
	//	log.Printf("Unable to upload  %s to s3://%s/%s, %v", filename, config.Aws.Bucket, filenameDst, err)
	//	return err
	//}

}

func backfill_db() {

	conn, err := pgxpool.Connect(context.Background(), config.Timescale.ConnectionString)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}

	ctx := context.Background()

	tempTable := config.Timescale.Table + "_backfill"
	createTable := "CREATE TABLE " + tempTable + " AS select time, log_entry_hash,log_entry from " + config.Timescale.Table + " WITH NO data"
	//createTable := "CREATE TABLE " + tempTable + " AS select * from "+ config.Timescale.Table + " WITH NO data"

	_, err = conn.Exec(ctx, createTable)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	rows := readLines(fullFileName)

	var cnt int64
	cnt, err = conn.CopyFrom(
		ctx,
		pgx.Identifier{tempTable},
		config.Timescale.ColumnNames,
		pgx.CopyFromSlice(len(rows), func(i int) ([]interface{}, error) {
			return []interface{}{rows[i].Timestamp, rows[i].Hash, []byte(rows[i].RawJsonString)}, nil
		}),
	)

	if err != nil { // no need to panic here, if something is wrong with a DB other tnah context cancel, then we keep prosessing data
		log.Printf("Unable to copy %d rows to TimescaleDB, %v\n", len(rows), err)
	} else {
		log.Printf("Coppied %d rows to %s.\n", cnt, config.Timescale.Table)
	}

	//dropTable := "Drop TABLE " + tempTable
	//_, err = conn.Exec(context.Background(), dropTable)
	//if err != nil {
	//	fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
	//	os.Exit(1)
	//}
	return

	//err := addToDB(array, context.Background())
	//if err != nil {
	//	fmt.Println("error %v", err, array)
	//	//return err
	//}

	//// go trough remote files
}
