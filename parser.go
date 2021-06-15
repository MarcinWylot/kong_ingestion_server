package main

import (
	"encoding/json"
	"fmt"
	"time"
)

type logentry struct {
	Timestamp     time.Time
	RawJsonString string
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
	return nil
}
