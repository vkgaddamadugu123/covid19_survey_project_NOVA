# COVID-19 Data Ingestion Pipeline

A robust, scalable data ingestion system that extracts COVID-19 case surveillance data from the CDC's public API, processes and cleans the data, and loads it into PostgreSQL for analysis.

![Python](https://img.shields.io/badge/python-v3.7+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?logo=postgresql&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## 📋 Table of Contents

- [High-Level Architecture](#high-level-architecture)
- [System Components](#system-components)
- [Data Flow Architecture](#data-flow-architecture)
- [Project Structure](#project-structure)
- [Detailed Data Flows](#detailed-data-flows)
- [Data Processing Features](#data-processing-features)
- [Setup and Installation](#setup-and-installation)
- [Usage Examples](#usage-examples)
- [Testing](#testing)
- [Configuration](#configuration)

## 🏗️ High-Level Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CDC COVID-19  │    │   Data Ingestion │    │   PostgreSQL    │
│   Public API    │───▶│     Pipeline     │───▶│    Database     │
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │  Data Processing │
                       │   & Validation   │
                       └──────────────────┘
```

### Architecture Principles

- **Resilient**: Built-in error handling and retry mechanisms
- **Scalable**: Pagination support for large datasets (100k+ records)
- **Auditable**: Dual-table architecture for data lineage
- **Testable**: Comprehensive unit test coverage
- **Configurable**: Environment-based configuration

## 🔧 System Components

### Core Components

| Component | File | Responsibility |
|-----------|------|----------------|
| **Data Ingestion Engine** | `Ingestion.py` | Main orchestration and API interaction |
| **Test Suite** | `ingtest.py` | Comprehensive unit testing |
| **Documentation** | `readme.txt` | Quick setup and usage guide |

### Key Modules

#### 1. Database Connection (`get_engine()`)
- PostgreSQL connection management
- Environment variable validation
- Connection pooling with health checks

#### 2. API Client (`fetch_page()`)
- CDC API interaction with pagination
- Rate limiting handling (429 errors)
- Request timeout management

#### 3. Data Transformation (`transform_rows_to_df()`)
- JSON to DataFrame conversion
- Schema validation and enforcement
- Missing column handling

#### 4. Data Cleaning (`clean_df()`)
- Duplicate removal
- Date standardization
- Text normalization
- Null value handling

#### 5. Data Loading
- `load_df_to_postgres_raw()`: Raw data landing
- `load_df_to_postgres()`: Clean data storage

## 🔄 Data Flow Architecture

```mermaid
graph TD
    A[CDC API] --> B[API Client]
    B --> C[Raw Data Validation]
    C --> D[PostgreSQL Raw Table]
    D --> E[Data Retrieval]
    E --> F[Data Cleaning]
    F --> G[Quality Validation]
    G --> H[PostgreSQL Clean Table]
    
    I[Error Handling] --> B
    I --> F
    I --> H
    
    J[Rate Limiting] --> B
    K[Batch Processing] --> B
    K --> F
    K --> H
```

### Data Flow Stages

1. **Extraction**: Paginated API calls to CDC endpoint
2. **Raw Storage**: Immediate persistence with ingestion timestamps
3. **Transformation**: Data cleaning and standardization
4. **Validation**: Quality checks and error handling
5. **Loading**: Final storage in analytical table

## 📁 Project Structure

```
Ingestion_Final/
├── 📄 Ingestion.py                 # Main ingestion pipeline
├── 📄 ingtest.py                   # Comprehensive test suite
├── 📄 readme.txt                   # Quick setup guide
├── 📄 README.md                    # This documentation
├── 📄 TECHNICAL_DOCUMENTATION.md   # Detailed technical docs
└── 📄 requirements.txt             # Python dependencies
```

### File Descriptions

- **`Ingestion.py`**: Core pipeline with data ingestion, transformation, and loading logic
- **`ingtest.py`**: pytest-based test suite covering all major functions
- **`readme.txt`**: Quick reference for Google Colab execution
- **`README.md`**: Comprehensive project documentation
- **`TECHNICAL_DOCUMENTATION.md`**: In-depth technical specifications

## 📊 Detailed Data Flows

### 1. Data Ingestion Pipeline

```python
# High-level pipeline flow
while total_fetched < max_records:
    # 1. Fetch data from API
    rows = fetch_page(limit=page_size, offset=offset)
    
    # 2. Convert to DataFrame
    df = transform_rows_to_df(rows)
    
    # 3. Add ingestion timestamp
    df["_ingested_at"] = pd.Timestamp.utcnow()
    
    # 4. Store raw data
    load_df_to_postgres_raw(engine, df)
    
    # 5. Retrieve and clean
    df_clean = clean_df(df_raw_from_pg)
    
    # 6. Store clean data
    load_df_to_postgres(engine, df_clean)
```

#### Pipeline Configuration

```python
# Default configuration
page_size = 50000      # Records per API call
max_records = 150000   # Total records to process
timeout = 60           # Request timeout (seconds)
delay = 0.7           # Inter-request delay (seconds)
```

### 2. Test Pipeline

```python
# Test execution flow
pytest ingtest.py
│
├── test_get_engine_env_missing()     # Environment validation
├── test_get_engine_valid()           # Connection testing
├── test_fetch_page_success()         # API interaction
├── test_fetch_page_rate_limit()      # Error handling
├── test_transform_rows_to_df()       # Data transformation
├── test_clean_df_basic()             # Data cleaning
├── test_load_df_to_postgres()        # Data loading (clean)
└── test_load_df_to_postgres_raw()    # Data loading (raw)
```

### 3. Data Processing Features

#### Data Cleaning Operations

| Operation | Implementation | Purpose |
|-----------|----------------|---------|
| **Deduplication** | `df.drop_duplicates()` | Remove identical records |
| **Date Parsing** | `pd.to_datetime()` | Convert date strings to datetime |
| **Text Standardization** | `.str.strip().str.upper()` | Normalize state codes |
| **Null Handling** | Replace variants with "Unknown" | Consistent categorical values |

#### Data Quality Checks

```python
# Schema validation
assert list(df.columns) == KEEP_COLS

# Data type validation
assert pd.api.types.is_datetime64_any_dtype(df["cdc_case_earliest_dt"])

# Content validation
assert df["res_state"].str.len().max() <= 2  # State code validation
```

## 🚀 Setup and Installation

### Prerequisites

- Python 3.7+
- PostgreSQL database access
- Internet connectivity for API access

### Quick Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd Ingestion_Final
```

2. **Install dependencies**
```bash
pip install pandas sqlalchemy psycopg2-binary requests pytest
```

3. **Configure database connection**
```python
import os
os.environ["PG_URL"] = "postgresql+psycopg2://user:pass@host:port/db?sslmode=require"
```

4. **Run the pipeline**
```bash
python Ingestion.py
```

### Google Colab Setup

```python
# Install dependencies
!pip -q install pandas sqlalchemy psycopg2-binary requests matplotlib pytest

# Set database connection
import os
os.environ["PG_URL"] = "postgresql+psycopg2://neondb_owner:npg_2wdZpRaF0LmG@ep-patient-dawn-advbhv1g-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"

# Run ingestion
!python Ingestion.py
```

## 💡 Usage Examples

### Basic Ingestion

```python
from Ingestion import main

# Run with default configuration
main()
```

### Custom Configuration

```python
import Ingestion

# Modify configuration
Ingestion.page_size = 25000
Ingestion.max_records = 100000

# Run ingestion
Ingestion.main()
```

### Data Verification

```python
from sqlalchemy import create_engine, text
import os

engine = create_engine(os.environ["PG_URL"])
with engine.connect() as conn:
    # Check record counts
    raw_count = conn.execute(text("SELECT COUNT(*) FROM covid_case_surveillance_raw")).scalar()
    clean_count = conn.execute(text("SELECT COUNT(*) FROM covid_case_surveillance")).scalar()
    
    print(f"Raw records: {raw_count}")
    print(f"Clean records: {clean_count}")
    
    # Check state distribution
    states = conn.execute(text("SELECT COUNT(DISTINCT res_state) FROM covid_case_surveillance")).scalar()
    print(f"Unique states: {states}")
```

### Data Analysis Examples

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(os.environ["PG_URL"])

# Load data for analysis
df = pd.read_sql_query("SELECT * FROM covid_case_surveillance LIMIT 10000", engine)

# Basic statistics
print(df['res_state'].value_counts().head())
print(df['age_group'].value_counts())
print(df['death_yn'].value_counts())
```

## 🧪 Testing

### Running Tests

```bash
# Run all tests
python ingtest.py

# Run with pytest directly
pytest ingtest.py -v

# Run specific test
pytest ingtest.py::test_clean_df_basic -v
```

### Test Coverage

| Test Function | Coverage | Purpose |
|---------------|----------|---------|
| `test_get_engine_env_missing` | Database connection | Environment validation |
| `test_get_engine_valid` | Database connection | Connection creation |
| `test_fetch_page_success` | API client | Successful API calls |
| `test_fetch_page_rate_limit` | API client | Rate limiting handling |
| `test_transform_rows_to_df` | Data transformation | DataFrame creation |
| `test_clean_df_basic` | Data cleaning | Cleaning operations |
| `test_load_df_to_postgres` | Data loading | Clean data storage |
| `test_load_df_to_postgres_raw` | Data loading | Raw data storage |

### Test Data Examples

```python
# Sample test data structure
test_data = [
    {
        "cdc_case_earliest_dt": "2020-07-01",
        "case_month": "2020-07",
        "res_state": " ny ",
        "age_group": "18-29",
        "sex": "Female",
        "race": "White",
        "ethnicity": "Non-Hispanic",
        "death_yn": " N ",
        "hosp_yn": " Y ",
        "icu_yn": "N",
        "medcond_yn": "Yes"
    }
]
```

## ⚙️ Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PG_URL` | Yes | None | PostgreSQL connection string |

### Configuration Parameters

```python
# API Configuration
API_URL = "https://data.cdc.gov/resource/n8mc-b4w4.json"

# Data Schema
KEEP_COLS = [
    "case_month", "cdc_case_earliest_dt", "res_state",
    "age_group", "sex", "race", "ethnicity",
    "death_yn", "hosp_yn", "icu_yn", "medcond_yn"
]

# Processing Configuration
page_size = 50000      # Batch size for API calls
max_records = 150000   # Maximum records to process
timeout = 60           # HTTP timeout in seconds
retry_delay = 2        # Delay before retry on rate limit
inter_request_delay = 0.7  # Delay between requests
```

## THANKYOU


