-- here we place raw logs, hot table, short term
-- retention 28 days, chunks & compression 6h 
CREATE TABLE public.events_raw (
  time TIMESTAMPTZ NOT NULL,
  consumer_id character varying DEFAULT NULL, 
  log_entry JSONB NOT NULL
);
SELECT create_hypertable('events_raw', 'time',  chunk_time_interval => INTERVAL '6 hours');

ALTER TABLE events_raw SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'consumer_id',
  timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('events_raw', INTERVAL '6 hours');
SELECT add_retention_policy('events_raw', INTERVAL '28 days');

-- before inserting we need to extract consumer_id, it is used for segmenting compressed data
CREATE OR REPLACE FUNCTION event_parser_before() RETURNS trigger AS $$
begin
    NEW.consumer_id = NEW.log_entry::json->'consumer'->>'custom_id';
    IF NEW.consumer_id is NOT NULL THEN
        RETURN NEW;
    ELSE
        RETURN NULL;
    END if;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER event_parser_before BEFORE INSERT ON "events_raw" FOR EACH ROW EXECUTE PROCEDURE event_parser_before();



-- here we place parsed logs, basically information extracted from JSON
-- should be used as base for continous aggregate
-- avoid using directlly, still expensive 
-- no retention policy, chunks & compression 30 days 
-- can be extended if more fields are needed from logs 
CREATE TABLE public.events_parsed (
  time TIMESTAMPTZ NOT NULL,
  consumer_id character varying DEFAULT NULL, 
  service_slug character varying DEFAULT NULL, 
  latency INT DEFAULT NULL,
  status SMALLINT DEFAULT NULL
);
SELECT create_hypertable('events_parsed', 'time',  chunk_time_interval => INTERVAL '30 days');
CREATE INDEX ON events_parsed (consumer_id, time desc);
CREATE INDEX ON events_parsed (service_slug, time DESC);
CREATE INDEX ON events_parsed (status, time DESC);

ALTER TABLE events_parsed SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'consumer_id',
  timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('events_parsed', INTERVAL '30 days');


-- after inserting raw log we can spend some time on parsing them
-- we basically extract what we need from JSON
CREATE OR REPLACE FUNCTION event_parser_after() RETURNS trigger AS $$
begin
	
  INSERT INTO events_parsed VALUES (
    NEW.id, NEW.time, NEW.consumer_id,
    NEW.log_entry::json->'service'->>'name',
    CAST (CAST (NEW.log_entry::json->'latencies'->>'request' as double precision) AS INTEGER ), 
    CAST (NEW.log_entry::json->'response'->>'status'AS SMALLINT )
  );
 
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER event_parser_after AFTER INSERT ON "events_raw" FOR EACH ROW EXECUTE PROCEDURE event_parser_after();
