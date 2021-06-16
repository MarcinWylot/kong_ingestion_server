package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	configFile = "config.cfg"
)

var server http.Server
var shutdownWG sync.WaitGroup

var config = struct {
	Server struct {
		TimeoutSecs   time.Duration
		Address       string
		numberOfCores int
	}
	File struct {
		Path            string
		Name            string
		RotateInterval  int
		BufferIncrement int
	}
	Aws struct {
		Switch            bool
		Bucket            string
		BucketFolder      string
		AccessKeyId       string
		SecretAccessKey   string
		Region            string
		RemoveSentFile    bool
		InactivityTimeout time.Duration
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
func timeMeasurement(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("[%s] execution time: %s", name, elapsed)
}

func init() {
	return
}

func homeView(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Kong Ingestion Server")
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	var err error
	message := "Kong Ingestion Server \n"
	if config.Aws.Switch == true {
		err2 := checkS3()
		if err2 != nil {
			message += "AWS connection FAILED.\n"
			err = err2
		} else {
			message += "AWS connection OK\n"
		}
	}

	if config.Timescale.Switch == true {
		err2 := checkTimescale(false)
		if err2 != nil {
			message += "Timescale connection FAILED\n"
			err = err2
		} else {
			message += "Timescale connection OK\n"
		}
	}
	if err != nil {
		http.Error(w, message, http.StatusInternalServerError)
		return
	}

	io.WriteString(w, message)

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

	rotate(config.File.RotateInterval, false)

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
	http.HandleFunc("/health", healthCheck)

	konglogsHandlerFunction := http.HandlerFunc(konglogs)
	konglogsHandlerFunctionWithTimeout := http.TimeoutHandler(konglogsHandlerFunction, config.Server.TimeoutSecs, "server timeout")
	http.Handle("/konglogs", konglogsHandlerFunctionWithTimeout)
}

func runServer() {
	shutdownWG.Add(1)
	defer shutdownWG.Done()
	setHandlers()

	server.Addr = config.Server.Address
	server.Handler = nil
	server.ReadTimeout = config.Server.TimeoutSecs / 2
	server.WriteTimeout = config.Server.TimeoutSecs * 2
	server.MaxHeaderBytes = 1 << 20

	log.Println("Opening for connections....")
	err := server.ListenAndServe()
	if err != nil {
		log.Println("HTTP Server: ", err)
	}
}

func shutdown() {
	//close timescaleDB connections
	if config.Timescale.Switch == true {
		log.Println("Closing DB pool.")
		dbPool.Close()
	}

	//close HTTP server
	log.Println("Closing HTTP server.")
	if err := server.Shutdown(context.Background()); err != nil {
		log.Printf("HTTP server Shutdown failed: %v", err)
	}

	//rotate and send to S3
	if config.Aws.Switch == true {
		log.Println("S3 graceful handling.")
		rotate(10, true)
	}

}

func setupGracefulShutdown(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		stop := make(chan os.Signal)
		signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

		defer signal.Stop(stop)
		defer wg.Done()
		sig := <-stop
		log.Println("Graceful shutdown begins. Signal: ", sig)
		shutdown()
		log.Println("Graceful shutdown done.")
	}()
}

func setupRotateToS3onSignal(wg *sync.WaitGroup) {
	for {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGUSR1)
		sig := <-c
		log.Println("Rotating file. Signal: ", sig)
		wg.Add(1)
		rotate(10, true)
		wg.Done()
	}
}

func rotateToS3onInactivity(wg *sync.WaitGroup) {
	var lastTimestamp time.Time
	for {
		lastTimestamp = globalLastTimesatmp
		time.Sleep(config.Aws.InactivityTimeout)
		log.Println("Checking Inactivity timeout: ", !globalLastTimesatmp.After(lastTimestamp))
		if !globalLastTimesatmp.After(lastTimestamp) {
			wg.Add(1)
			log.Println("Inactivity timeout triggered: ", config.Aws.InactivityTimeout)
			rotate(10, true)
			wg.Done()
		}

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
		err := checkTimescale(true)
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

	setupGracefulShutdown(&shutdownWG)
	if config.Aws.Switch {
		go setupRotateToS3onSignal(&shutdownWG)
		if config.Aws.InactivityTimeout > 0 {
			go rotateToS3onInactivity(&shutdownWG)
		}
	}
}

func main() {
	startup()

	runServer()

	shutdownWG.Wait()
	log.Println("Bye Bye....")
}
