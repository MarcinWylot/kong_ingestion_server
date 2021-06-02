package main

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"strings"
	"time"
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

func checkTimescale(full bool) error {
	var t time.Time
	err := dbPool.QueryRow(context.Background(), "select NOW()").Scan(&t)
	if err != nil {
		log.Printf("Timescale connection check error: %v\n", err)
		return err
	}

	if full {
		query := "select " + strings.Join(config.Timescale.ColumnNames, ",") + " from " + config.Timescale.Table + " limit 1"
		_, err = dbPool.Query(context.Background(), query)
		if err != nil {
			log.Printf("Timescale table check error: %v\n", err)
			return err
		}
	}

	return nil
}
