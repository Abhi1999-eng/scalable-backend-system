# Scalable Backend System

This project now supports two related data pipelines:

- high-throughput file ingestion for bulk file uploads
- dataset analytics for CSV uploads, schema profiling, sample row browsing, and chart-ready aggregations

## What it can do

- Accept many multipart files in a single request with `POST /ingestion/files/bulk`
- Accept a ZIP archive containing thousands of files with `POST /ingestion/files/zip`
- Process uploads asynchronously so the request returns immediately with an `uploadId`
- Persist raw files to local disk under `uploads/<uploadId>/`
- Batch-insert file metadata into PostgreSQL for faster storage throughput
- Track processing progress with `GET /ingestion/files/{uploadId}`
- Accept CSV datasets with `POST /datasets/upload`
- Track dataset ingestion with `GET /datasets/{datasetId}/status`
- Inspect inferred schema with `GET /datasets/{datasetId}/schema`
- Inspect dataset profile with `GET /datasets/{datasetId}/profile`
- Preview stored rows with `GET /datasets/{datasetId}/rows`
- Generate chart-ready aggregates with `POST /datasets/{datasetId}/charts`

## Why this is faster for 10k+ files

- Upload processing runs on a dedicated executor instead of the request thread
- Multipart uploads are processed in chunks to improve parallelism
- Metadata is inserted with JDBC batch writes instead of one row at a time
- Files are streamed to disk with a 64 KB buffer, so the app does not load whole files into memory
- ZIP uploads let you move 10k+ files in one request instead of sending 10k form parts individually

## Recommended approach

For the best throughput, upload a ZIP file with `POST /ingestion/files/zip`.

That keeps HTTP overhead low and is the most realistic way to send 10,000+ files in a single request.

## API

### Upload many files

```bash
curl -X POST http://localhost:8082/ingestion/files/bulk \
  -F "files=@/path/to/file1.txt" \
  -F "files=@/path/to/file2.txt"
```

### Upload a ZIP archive

```bash
curl -X POST http://localhost:8082/ingestion/files/zip \
  -F "archive=@/path/to/files.zip"
```

### Check upload status

```bash
curl http://localhost:8082/ingestion/files/<uploadId>
```

### Upload a dataset

```bash
curl -X POST http://localhost:8082/datasets/upload \
  -F "file=@/path/to/dataset.csv"
```

### Get dataset status

```bash
curl http://localhost:8082/datasets/<datasetId>/status
```

### Get dataset schema

```bash
curl http://localhost:8082/datasets/<datasetId>/schema
```

### Get dataset profile

```bash
curl http://localhost:8082/datasets/<datasetId>/profile
```

### Preview dataset rows

```bash
curl "http://localhost:8082/datasets/<datasetId>/rows?limit=25&offset=0"
```

### Build a chart

```bash
curl -X POST http://localhost:8082/datasets/<datasetId>/charts \
  -H "Content-Type: application/json" \
  -d '{
    "chartType": "bar",
    "xColumn": "region",
    "aggregation": "count",
    "limit": 8
  }'
```

## Stored metadata

Each processed file is inserted into the `stored_files` table with:

- `upload_id`
- `original_filename`
- `stored_filename`
- `storage_path`
- `content_type`
- `size_bytes`
- `line_count`
- `checksum_sha256`
- `status`
- `created_at`

## Local setup

Make sure these are running before starting the app:

- PostgreSQL on `localhost:5432`
- Redis on `localhost:6379`
- Kafka on `localhost:9092`

Then run:

```bash
./mvnw spring-boot:run
```

## Frontend

A Next.js dashboard is available in [frontend](/Users/abhishekchaubey/scalable-backend-system/frontend).

Run it locally in a second terminal:

```bash
cd frontend
npm install
cp .env.example .env.local
npm run dev
```

Open `http://localhost:3000`.

The frontend proxies requests through Next route handlers, so browser calls stay same-origin while the app forwards traffic to the Spring backend at `BACKEND_URL`.

The current frontend is focused on dataset analytics:

- upload a CSV dataset
- watch async ingestion status
- inspect inferred schema and profile
- preview sample rows
- build a simple chart from uploaded data

## Free deploy path

The simplest low-cost setup for this repo is:

- frontend on Vercel Hobby
- backend on Render
- PostgreSQL on Neon or Render Postgres
- Redis on Upstash Redis or Render Key Value

For a lightweight demo deployment, set:

```bash
APP_KAFKA_ENABLED=false
```

and provide these backend environment variables:

```bash
SPRING_DATASOURCE_URL=...
SPRING_DATASOURCE_USERNAME=...
SPRING_DATASOURCE_PASSWORD=...
SPRING_DATA_REDIS_URL=...
APP_AUTH_JWT_SECRET=...
```

The frontend needs:

```bash
BACKEND_URL=https://your-backend-url
```

This disables Kafka-only features for the hosted demo while keeping dataset upload, analytics, auth, and saved charts working.
