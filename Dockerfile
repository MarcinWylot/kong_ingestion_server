FROM scratch
COPY kong_ingestion_server /kong_ingestion_server
ENTRYPOINT ["/kong_ingestion_server"]
