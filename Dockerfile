FROM alpine
RUN apk --no-cache add ca-certificates
COPY kong_ingestion_server /kong_ingestion_server
ENTRYPOINT ["/kong_ingestion_server"]
