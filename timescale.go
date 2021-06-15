package main

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"strings"
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
			return []interface{}{rows[i].Timestamp, []byte(rows[i].RawJsonString)}, nil
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
	_, err := dbPool.Exec(context.Background(), "select NOW()")
	if err != nil {
		log.Printf("Timescale connection check error: %v\n", err)
		return err
	}

	if full {
		query := "select " + strings.Join(config.Timescale.ColumnNames, ",") + " from " + config.Timescale.Table + " limit 1"
		_, err = dbPool.Exec(context.Background(), query)
		if err != nil {
			log.Printf("Timescale table check error: %v\n", err)
			return err
		}
	}

	return nil
}
