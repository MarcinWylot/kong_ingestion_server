<!-- ABOUT THE PROJECT -->
## About Kong Ingestion Server

Kong Ingestion Server (KIS) is an HTTP server in Go serving as a bridge between Kong Gateway HTTP log plugin and TimescaleDB. KIS received batched of logs in a form of JSON objects, parses them and stores in the database, local filesystem, and S3 bucket. 

![Architecture](architecture.svg)


## Getting Started

To get a local copy up and running follow these simple steps.

### Prerequisites

1. [Golang](https://golang.org/)
2. [TimescaleDB](https://timescale.com/)
3. [Kong Gateway OSS](https://konghq.com/)


### Installation

1. [Install Golang](https://golang.org/doc/install)
2. [Install TimescaleDB](https://docs.timescale.com/latest/main)
3. Create database ```CREATE DATABASE kis;``` and [tables](./tables.sql) 
4. Clone Kong Ingestion Server
   ```sh
   git clone https://github.com/MarcinWylot/kong_ingestion_server.git
   ```
5. Compile Kong Ingestion Server
   ```sh
   go build .
   ```
   If you prefer to compile for docker
    ```sh
   CGO_ENABLED=0 GOOS=linux go build .
   ```

6. Adjust [config](./config.cfg)
7. Setup [HTTP Log](https://docs.konghq.com/hub/kong-inc/http-log/) within your Kong Gateway configuration.
7. Run Kong Ingestion Server
   ```sh
   ./kong_ingestion_server
   ```

