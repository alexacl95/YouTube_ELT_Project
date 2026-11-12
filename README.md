# YouTube ELT Project

An Extract-Load-Transform (ELT) data pipeline that extracts YouTube video statistics using the YouTube API, loads the raw data into a staging layer, transforms it into a core layer, and validates data quality. Built with Apache Airflow 3.3.2, PostgreSQL, Redis, and Soda for data quality checks.
Based on [MattTheDataEngineer](https://github.com/MattTheDataEngineer) course.

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Installation & Setup](#installation--setup)
- [Configuration](#configuration)
- [DAGs & Workflows](#dags--workflows)
- [Database Schema](#database-schema)
- [Data Quality Checks](#data-quality-checks)
- [Running the Project](#running-the-project)
- [Testing](#testing)

---

## Project Overview

This project implements an automated ELT pipeline that:

1. **Extracts** video statistics from YouTube using the YouTube Data API v3
2. **Loads** raw data into a PostgreSQL staging schema
3. **Transforms** staged data into a core schema with enriched dimensions
4. **Validates** data quality at both layers using Soda
5. **Orchestrates** the entire workflow using Apache Airflow with CeleryExecutor

### Key Features

- Multi-layer data architecture (staging → core)
- Automated data quality validation
- Incremental data loading with upsert logic
- Full containerization with Docker Compose
- Airflow 3.3.2 with modern features (api-server, dag-processor)
- Redis-backed Celery executor for distributed task processing

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│         YouTube Data API v3                         │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│  DAG 1: produce_json                                │
│  Extracts video data → saves to JSON                │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│  DAG 2: update_db                                   │
│  staging.yt_api ◄─ raw JSON                         │
│  core.yt_api    ◄─ transformed staging              │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│  DAG 3: data_quality                                │
│  Soda validation on staging & core layers           │
└─────────────────────────────────────────────────────┘
```

### Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Apache Airflow | 3.3.2 | Orchestration & workflow management |
| PostgreSQL | 16 | Data storage |
| Redis | 7.2-bookworm | Message broker for Celery |
| Soda Core | 4.28.0 | Data quality validation |
| Python | 3.12 | DAG & task implementation |
| Docker | Latest | Containerization |

---

## Project Structure

```
YouTube_ELT_project/
├── dags/
│   ├── main.py                          # DAG definitions (3 workflows)
│   │
│   ├── api/
│   │   └── video_stats.py               # YouTube API extraction tasks
│   │
│   ├── dataquality/
│   │   └── soda.py                      # Data quality validation tasks
│   │
│   └── datawarehouse/
│       ├── dwh.py                       # Staging & core table tasks
│       ├── data_loading.py              # JSON file loading
│       ├── data_modification.py         # CRUD operations (insert/update/delete)
│       ├── data_transformation.py       # Data enrichment & transformation
│       └── data_utils.py                # DB connections & schema/table creation
│
├── include/
│   └── soda/
│       ├── checks.yml                   # Data quality rules
│       └── configuration.yml            # Soda Postgres connection config
│
├── docker/
│   └── postgres/
│       └── init-multiple-databases.sh   # PostgreSQL multi-DB initialization
│
├── tests/
│   ├── conftest.py                      # Pytest configuration & fixtures
│   ├── unit_test.py                     # Unit tests for utility functions
│   └── integration_test.py              # Integration tests with test DB
│
├── config/                              # (Reserved for Airflow configs)
├── plugins/                             # (Reserved for Airflow plugins)
├── data/                                # JSON files output directory
├── logs/                                # Airflow task logs
│
├── dockerfile                           # Custom Airflow image
├── docker-compose.yaml                  # Multi-container orchestration
├── requirements.txt                     # Python dependencies
└── README.md                            # This file
```

---

## Installation & Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd YouTube_ELT_project
```

### 2. Create `.env` File

Create a `.env` file in the project root with the following variables:

```env
# Airflow
AIRFLOW_UID=50000
AIRFLOW_WWW_USER_USERNAME=airflow
AIRFLOW_WWW_USER_PASSWORD=airflow
AIRFLOW__API_AUTH__JWT_SECRET=airflow_jwt_secret
AIRFLOW__API_AUTH__JWT_ISSUER=airflow
AIRFLOW__API__SECRET_KEY=airflow_secret_key

# PostgreSQL - Metadata DB (Airflow internal)
POSTGRES_CONN_USERNAME=postgres
POSTGRES_CONN_PASSWORD=postgres
POSTGRES_CONN_HOST=postgres
POSTGRES_CONN_PORT=5432
METADATA_DATABASE_NAME=airflow_db
METADATA_DATABASE_USERNAME=airflow_user
METADATA_DATABASE_PASSWORD=airflow_password

# PostgreSQL - ELT Data DB (YouTube data)
ELT_DATABASE_NAME=elt_db
ELT_DATABASE_USERNAME=elt_user
ELT_DATABASE_PASSWORD=elt_password

# PostgreSQL - Celery Backend DB
CELERY_BACKEND_NAME=celery_db
CELERY_BACKEND_USERNAME=celery_user
CELERY_BACKEND_PASSWORD=celery_password

# Fernet Key (for secret encryption)
FERNET_KEY=<generated-fernet-key>

# Docker Hub
DOCKERHUB_NAMESPACE=<your-namespace>
DOCKERHUB_REPOSITORY=<your-repo>

# YouTube API
API_KEY=<your-youtube-api-key>
CHANNEL_HANDLE=<your-channel-handle>  # e.g., @mychannel
```

### 3. Generate Fernet Key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 4. Build Custom Airflow Image

```bash
docker build -t <your-namespace>/<your-repo>:latest .
```

### 5. Start Services

```bash
docker compose up -d
```

Monitor initialization:

```bash
docker compose logs -f airflow-init
```

---

## Configuration

### YouTube API Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project
3. Enable YouTube Data API v3
4. Create an OAuth 2.0 credential (API key)
5. Add `API_KEY` to `.env`
6. Get your channel handle (format: `@channelname`)

### Airflow Connections

The project uses **environment-based configuration**:

- **Connection**: `AIRFLOW_CONN_POSTGRES_DB_YT_ELT` → Read as `postgres_db_yt_elt`
- **Variables**: `AIRFLOW_VAR_API_KEY`, `AIRFLOW_VAR_CHANNEL_HANDLE` → Read via `Variable.get()`

### Soda Configuration

File: `include/soda/configuration.yml`

```yaml
type: postgres
name: pg_datasource
connection:
  host: ${env.POSTGRES_CONN_HOST}
  port: ${env.POSTGRES_CONN_PORT}
  database: ${env.ELT_DATABASE_NAME}
  user: ${env.ELT_DATABASE_USERNAME}
  password: ${env.ELT_DATABASE_PASSWORD}
```

---

## DAGs & Workflows

### DAG 1: `produce_json`

**Schedule**: Daily at 10:00 AM (UTC: `0 10 * * *`)

**Purpose**: Extract YouTube video data and save to JSON

**Tasks**:
1. **get_playlist_id** → Retrieves the uploads playlist ID for the channel
2. **get_video_ids** → Paginated retrieval of all video IDs from the playlist
3. **extract_video_data** → Fetches detailed stats (views, likes, comments) in batches
4. **save_to_json_task** → Saves extracted data to `data/YT_data_YYYY-MM-DD.json`
5. **trigger_update_db** → Triggers DAG 2 upon completion

**Output**: `data/YT_data_{date}.json`

```json
[
  {
    "video_id": "dQw4w9WgXcQ",
    "title": "Video Title",
    "publishedAt": "2024-01-01T12:00:00Z",
    "duration": "PT3M54S",
    "viewCount": "1000000",
    "likeCount": "50000",
    "commentCount": "10000"
  }
]
```

---

### DAG 2: `update_db`

**Schedule**: Triggered by DAG 1 completion

**Purpose**: Load & transform data into staging and core schemas

**Tasks**:

#### 2.1 staging_table()
- **Schema**: `staging.yt_api`
- **Logic**:
  - Creates schema if not exists
  - Loads JSON data via `data_loading.py`
  - **Upsert logic**:
    - First run: INSERT all rows
    - Subsequent runs: UPDATE existing rows, INSERT new ones
  - Deletes rows no longer in JSON
- **Columns**:
  - `Video_ID` (PK, VARCHAR 11)
  - `Video_Title` (TEXT)
  - `Upload_Date` (TIMESTAMP)
  - `Duration` (VARCHAR 20, ISO 8601)
  - `Video_Views`, `Likes_Count`, `Comments_count` (INT, nullable)

#### 2.2 core_table()
- **Schema**: `core.yt_api`
- **Logic**:
  - Creates schema if not exists
  - Reads from `staging.yt_api`
  - Transforms each row via `transform_data()`
  - Applies same upsert logic as staging
- **Columns**:
  - `Video_ID` (PK, VARCHAR 11)
  - `Video_Title` (TEXT)
  - `Upload_Date` (TIMESTAMP)
  - `Duration` (TIME, parsed from ISO 8601)
  - `Video_type` (VARCHAR 10: "Shorts" if ≤60s, else "Normal")
  - `Video_Views`, `Likes_Count`, `Comments_count` (INT, nullable)

#### 2.3 trigger_data_quality
- Triggers DAG 3 upon completion

---

### DAG 3: `data_quality`

**Schedule**: Triggered by DAG 2 completion

**Purpose**: Validate data integrity at both layers

**Tasks**:

1. **soda_validate_staging** → Validates `staging.yt_api`
2. **soda_validate_core** → Validates `core.yt_api`

---

## Database Schema

### Metadata DB (airflow_db)

Internal Airflow metadata. Auto-created by Airflow.

### ELT DB (elt_db)

#### Schema: staging

```sql
CREATE TABLE staging.yt_api (
    "Video_ID"        VARCHAR(11) PRIMARY KEY NOT NULL,
    "Video_Title"     TEXT NOT NULL,
    "Upload_Date"     TIMESTAMP NOT NULL,
    "Duration"        VARCHAR(20) NOT NULL,
    "Video_Views"     INT,
    "Likes_Count"     INT,
    "Comments_count"  INT
);
```

**Purpose**: Raw data from YouTube API (ISO 8601 duration string)

#### Schema: core

```sql
CREATE TABLE core.yt_api (
    "Video_ID"        VARCHAR(11) PRIMARY KEY NOT NULL,
    "Video_Title"     TEXT NOT NULL,
    "Upload_Date"     TIMESTAMP NOT NULL,
    "Duration"        TIME NOT NULL,
    "Video_type"      VARCHAR(10) NOT NULL,
    "Video_Views"     INT,
    "Likes_Count"     INT,
    "Comments_count"  INT
);
```

**Purpose**: Transformed data with parsed duration and video type classification

### Celery Backend DB (celery_db)

Stores Celery task results. Auto-created.

---

## Data Quality Checks

File: `include/soda/checks.yml`

### Column-Level Checks

- **Video_ID**:
  - `missing` → No NULL values
  - `duplicate` → No duplicate IDs

### Row-Level Checks

- **likes_gt_views**: `Likes_Count > Video_Views` (should have zero failed rows)
- **comments_gt_views**: `Comments_count > Video_Views` (should have zero failed rows)

### Execution

```bash
soda contract verify \
  -ds include/soda/configuration.yml \
  -c include/soda/checks.yml \
  --set DATASET=staging \
  -v
```

---

## Running the Project

### Start Services

```bash
docker compose up -d
```

### Access Airflow UI

- **URL**: http://localhost:8080
- **Username**: `airflow` (or configured in `.env`)
- **Password**: `airflow` (or configured in `.env`)

### Trigger DAG Manually

```bash
# Trigger the produce_json DAG
docker compose exec airflow-scheduler airflow dags trigger produce_json

# Or via UI: Click "Trigger DAG" on the DAG
```

### View DAG Logs

```bash
# Logs for specific run
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker

# Or via UI: Click on DAG run → task → Log
```

### Query Results

```bash
docker compose exec postgres psql -U elt_user -d elt_db

# In psql:
SELECT * FROM staging.yt_api;
SELECT * FROM core.yt_api;
```

### Stop Services

```bash
docker compose down

# Remove volumes (careful: deletes all data)
docker compose down -v
```

---

## Testing

### Run Unit Tests

```bash
docker compose exec airflow-scheduler pytest tests/unit_test.py -v
```

### Run Integration Tests

```bash
docker compose exec airflow-scheduler pytest tests/integration_test.py -v
```

### Test Configuration

File: `tests/conftest.py`

Provides:
- Test database fixtures
- PostgreSQL connection helpers
- Mock Airflow Variable/Connection objects

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `dags/main.py` | DAG definitions & task dependencies |
| `dags/api/video_stats.py` | YouTube API integration |
| `dags/datawarehouse/dwh.py` | Staging/core table orchestration |
| `dags/datawarehouse/data_*.py` | Load, transform, CRUD operations |
| `include/soda/checks.yml` | Data quality rules |
| `docker-compose.yaml` | Multi-container orchestration |
| `dockerfile` | Custom Airflow image |
| `requirements.txt` | Python dependencies |

---