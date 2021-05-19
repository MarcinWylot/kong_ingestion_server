package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	dbPool *pgxpool.Pool
)

func addToDB(rows []logentry, ctx context.Context) error {
	cnt, err := dbPool.CopyFrom(
		ctx,
		pgx.Identifier{config.Timescale.Table},
		config.Timescale.ColumnNames,
		pgx.CopyFromSlice(len(rows), func(i int) ([]interface{}, error) {
			return []interface{}{rows[i].Timestamp, rows[i].Hash, []byte(rows[i].RawJsonString)}, nil
		}),
	)

	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	if err != nil { // no need to panic here, if something is wrong with a DB other tnah context cancel, then we keep prosessing data
		log.Printf("Unable to copy %d rows to TimescaleDB, %v\n", len(rows), err)
	} else {
		log.Printf("Coppied %d rows to %s.\n", cnt, config.Timescale.Table)
	}

	return nil
}

func timescaleConnect() {
	var err error
	dbPool, err = pgxpool.Connect(context.Background(), config.Timescale.ConnectionString)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
}

func test_db() {
	//run a simple query to check our connection
	var greeting string
	err := dbPool.QueryRow(context.Background(), "select 'Hello, Timescale!'").Scan(&greeting)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(greeting)
}
