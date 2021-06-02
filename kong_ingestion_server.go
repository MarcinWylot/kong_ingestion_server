package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	"io"
	"log"
	"net/http"
	"runtime"
	"strings"
	"time"
)

var (
	configFile = "./config.cfg"
)

var config = struct {
	Server struct {
		TimeoutSecs   time.Duration
		Addres        string
		numberOfCores int
	}
	File struct {
		Path            string
		Name            string
		RotateInterval  int
		BufferIncrement int
	}
	Aws struct {
		Switch          bool
		Bucket          string
		BucketFolder    string
		AccessKeyId     string
		SecretAccessKey string
		Region          string
	}
	Timescale struct {
		Switch           bool
		Table            string
		ColumnNames      []string
		ConnectionString string
	}
}{}

func getConfig(config interface{}, configFile string) {
	viper.SetConfigName(configFile)
	viper.SetConfigType("ini")
	viper.AddConfigPath("/")
	viper.AddConfigPath(".")

	viper.AutomaticEnv()
	viper.SetEnvPrefix("KIS")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file:\n %v", err)
	}

	err := viper.Unmarshal(&config)
	if err != nil {
		log.Fatalf("Error decoding config:\n %v", err)
	}

}

func init() {
	return
}

func homeView(w http.ResponseWriter, r *http.Request) {
	headers := w.Header()
	headers.Add("Content-Type", "text/html")
	io.WriteString(w, "<html><head></head><body><p>Kong to timescaleDB data ingestion server</p></body></html>")
}

func processLogs(data []byte, ctx context.Context) error {
	var array []logentry
	err := json.Unmarshal(data, &array)
	if err != nil {
		log.Printf("Error parsing JSON: %v ", err)
		return err
	}

	log.Printf("Storing %d entries\n", len(array))
	if config.Timescale.Switch == true {
		err = addToDB(array, ctx)
		if err != nil {
			return err
		}
	}

	err = appendToFile(array)
	if err != nil {
		return err
	}

	rotate()

	return nil

}

func konglogs(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		http.Error(w, "Only POST requests are allowed.", http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "Only application/json content is allowed.", http.StatusUnsupportedMediaType)
		return
	}

	b, err := io.ReadAll(io.LimitReader(r.Body, 100000000))
	if err != nil {
		log.Printf("Error reading data from an HTTP clinet, %v", err)
		http.Error(w, "Error reading data.", http.StatusInternalServerError)
		return
	}

	err = processLogs(b, r.Context())
	if err != nil {
		http.Error(w, "Error processing logs.", http.StatusInternalServerError)
		return
	}
}

func setHandlers() {
	http.HandleFunc("/", homeView)

	konglogsHandlerFunction := http.HandlerFunc(konglogs)
	konglogsHandlerFunctionWithTimeout := http.TimeoutHandler(konglogsHandlerFunction, config.Server.TimeoutSecs, "server timeout")
	http.Handle("/konglogs", konglogsHandlerFunctionWithTimeout)
}

func runServer() {
	setHandlers()

	server := &http.Server{
		Addr:           config.Server.Addres,
		Handler:        nil,
		ReadTimeout:    config.Server.TimeoutSecs / 2,
		WriteTimeout:   config.Server.TimeoutSecs * 2,
		MaxHeaderBytes: 1 << 20,
	}
	log.Println("Openning for connections....")
	err := server.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}

func startup() {
	getConfig(&config, configFile)

	runtime.GOMAXPROCS(config.Server.numberOfCores)

	fullFileName = fmt.Sprintf("%s/%s", config.File.Path, config.File.Name)
	openFile()
	rotateCounter = lineCounter(fullFileName)

	if config.Timescale.Switch == true {
		timescaleConnect()
		err := checkTimescale(false)
		if err != nil {
			log.Panicln("Timescale check failed")
		}
	}

	if config.Aws.Switch == true {
		err := checkS3()
		if err != nil {
			log.Panicln("AWS S3 check failed")
		}
	}
}

func main() {
	startup()

	runServer()
}
