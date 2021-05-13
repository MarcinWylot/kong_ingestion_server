package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	appendToFileMutex   sync.Mutex
	rotateFileMutex     sync.Mutex
	appendFileDesriptor *os.File
	fullFileName        string
	rotateCounter       = 0
)

func openFile() {
	var err error
	appendFileDesriptor, err = os.OpenFile(fullFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Panicf("Unable to open file %s, %v\n", fullFileName, err)
	}
}

func appendToFile(array []logentry) error {
	var raw_values strings.Builder
	raw_values.Grow(config.File.BufferIncrement)
	for k := range array {
		fmt.Fprintf(&raw_values, "%s\n", array[k].RawJsonString)
	}

	appendToFileMutex.Lock()
	defer appendToFileMutex.Unlock()
	_, err := appendFileDesriptor.WriteString(raw_values.String())
	if err != nil {
		log.Printf("Unable to write to %s, %v\n", fullFileName, err)
		return err
	}
	rotateCounter += len(array)
	log.Printf("Appended %d entries to %s\n", len(array), fullFileName)
	return nil
}

func lineCounter(file string) int {
	r, err := os.Open(file)
	defer r.Close()
	if err != nil {
		log.Fatalf("Unable to open file %s, %v\n", file, err)
	}

	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count
		case err != nil:
			log.Fatalf("Error reading from file %s, %v\n", file, err)
		}
	}
}

func rotate() {
	if rotateCounter >= config.File.RotateInterval {
		fullFileNameNew, _, timestamp, result := rotateFile()
		if result {
			go func() { // no need to wait for gzip and s3
				fullFileNameNewWithChecksum, err := renameWithChecksum(fullFileNameNew)
				if err == nil {
					fullFileNameNewGz, err := gzipFile(fullFileNameNewWithChecksum, "")
					if err == nil && config.Aws.Switch == true {
						sendToS3(fullFileNameNewGz, timestamp)
					}
				}
			}()
		}
	}
}

func ifFileExits(file_base string, cnt int) (string, string, error) {
	var file, file_gz string

	switch {
	case cnt == 0:
		file = file_base
		file_gz = fmt.Sprintf("%s.gz", file_base)
	default:
		file = fmt.Sprintf("%s.%d", file_base, cnt)
		file_gz = fmt.Sprintf("%s.%d.gz", file_base, cnt)
	}

	_, err := os.Stat(file)
	_, err_gz := os.Stat(file_gz)

	switch {
	case err == nil, err_gz == nil:
		cnt++
		return ifFileExits(file_base, cnt)
	case os.IsNotExist(err) && os.IsNotExist(err_gz):
		return file, file_gz, nil
	case err != nil:
		return file, file_gz, err
	case err_gz != nil:
		return file, file_gz, err_gz
	default:
		return file, file_gz, errors.New("Unknown Error")
	}
}

func renameWithChecksum(source string) (string, error) {
	// add checksum to the file name
	// helps to avoid collisions, and add extra control for files
	checksum, err := sha256sum(source)
	if err != nil {
		return "", err
	}
	sourceChecksum := fmt.Sprintf("%s.%s", source, checksum)
	err = os.Rename(source, sourceChecksum)
	if err != nil {
		return "", err
	}
	return sourceChecksum, err
}

func gzipFile(source, target_deprecated string) (string, error) {

	target := fmt.Sprintf("%s.gz", source)

	reader, err := os.Open(source)
	if err != nil {
		log.Printf("Unable to open file %s, %v\n", source, err)
		return target, err
	}
	defer reader.Close()

	writer, err := os.Create(target)
	if err != nil {
		log.Printf("Unable to open file %s, %v\n", target, err)
		return target, err
	}
	defer writer.Close()

	archiver := gzip.NewWriter(writer)
	archiver.Name = filepath.Base(source)
	defer archiver.Close()

	_, err = io.Copy(archiver, reader)
	if err == nil {
		err = os.Remove(source)
		log.Printf("Compressed data: %s\n", archiver.Name)
		return target, nil
	} else {
		log.Printf("Unable to write to file %s, %v\n", target, err)
		return target, err
	}
}

func sendToS3(source string, timestamp int64) error {
	filename := filepath.Base(source)

	t := time.Unix(timestamp, 0)
	date := fmt.Sprintf(t.Format("2006-01-02"))
	filenameDst := fmt.Sprintf("%s/%s/%s", config.Aws.BucketFolder, date, filename)

	file, err := os.Open(source)
	if err != nil {
		log.Printf("Unable to open file %s, %v\n", source, err)
		return err
	}
	defer file.Close()

	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(config.Aws.Region),
		Credentials: credentials.NewStaticCredentials(config.Aws.AccessKeyId, config.Aws.SecretAccessKey, ""),
	}))

	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(config.Aws.Bucket),
		Key:    aws.String(filenameDst),
		Body:   file,
	})
	if err != nil {
		log.Printf("Unable to upload  %s to s3://%s/%s, %v", filename, config.Aws.Bucket, filenameDst, err)
		return err
	}

	log.Printf("Uploaded %s to s3://%s/%s\n", filename, config.Aws.Bucket, filenameDst)
	return nil
}

func rotateFile() (string, string, int64, bool) {
	rotateFileMutex.Lock()
	defer rotateFileMutex.Unlock()

	var err error
	var fullFileNameNewGz string
	timestamp := time.Now().Unix()
	fullFileNameNew := fmt.Sprintf("%s.%d", fullFileName, timestamp)

	if rotateCounter >= config.File.RotateInterval {
		fullFileNameNew, fullFileNameNewGz, err = ifFileExits(fullFileNameNew, 0)
		if err != nil {
			return fullFileNameNew, fullFileNameNewGz, timestamp, false
		}

		appendToFileMutex.Lock()
		defer appendToFileMutex.Unlock()
		log.Printf("Rotating file to %s", fullFileNameNew)
		appendFileDesriptor.Close()
		err := os.Rename(fullFileName, fullFileNameNew)
		openFile() // irrespectivelly from os.Rename result the writer should be active for others
		if err != nil {
			log.Printf("Unable to rename %s to %s, %v\n", fullFileName, fullFileNameNew, err)
			return fullFileNameNew, fullFileNameNewGz, timestamp, false
		}
		rotateCounter = 0
		return fullFileNameNew, fullFileNameNewGz, timestamp, true
	}
	return fullFileNameNew, fullFileNameNewGz, timestamp, false
}

func readLines(file string) {
	r, err := os.Open(file)
	defer r.Close()
	if err != nil {
		log.Fatalf("Unable to open file %s, %v\n", file, err)
	}

	scanner := bufio.NewScanner(r)

	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		text := scanner.Text()

		var le logentry
		_ = json.Unmarshal([]byte(text), &le)

		log.Println("file: ", hash([]byte(text)), le.Hash, le.Timestamp)
	}

}

func sha256sum(file string) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		log.Printf("Unable to open file %s, %v\n", file, err)
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Printf("Unable to compute sha256 for file %s, %v\n", file, err)
		return "", err
	}

	checksum := fmt.Sprintf("%x", h.Sum(nil))
	return checksum, nil
}
