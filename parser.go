package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"
)

type logentry struct {
	Timestamp     time.Time
	Hash          []byte
	RawJsonString string
}

func hash(b []byte) []byte {
	hash := fnv.New128()
	hash.Write(b)
	return hash.Sum(nil)
}

func (r *logentry) UnmarshalJSON(p []byte) error {
	var tmp struct {
		Timestamp int64 `json:"started_at"`
	}
	if err := json.Unmarshal(p, &tmp); err != nil {
		return err
	}
	r.Timestamp = time.Unix(0, tmp.Timestamp*1000000)
	r.RawJsonString = fmt.Sprintf("%s", p)
	r.Hash = hash(p)
	return nil
}
