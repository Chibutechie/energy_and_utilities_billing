# Nigerian Energy & Utilities Billing Pipeline

A comprehensive ETL data pipeline that ingests, transforms, and loads Nigerian energy billing and payment data into PostgreSQL for analytics and reporting. This project empowers utilities companies, regulators, and analysts to gain actionable insights into billing efficiency, revenue collection, customer payment behaviors, and billing anomalies.

---

## Overview

### Problem Statement

The Nigerian electricity and utilities sector faces significant challenges:

- **Low collection efficiency**: Only 70-80% billing efficiency reported in many quarters
- **Estimated billing practices**: Widespread non-metered billing leading to inaccuracies
- **Revenue shortfalls**: Large gaps between billed amounts and actual collections
- **Limited transparency**: Insufficient data-driven insights for policy decisions

According to [Nairametrics](https://nairametrics.com/2022/09/25/discos-collected-n210-billion-out-of-n303-billion-billed-to-customers-in-q4-2021), Distribution Companies (DisCos) collected only ₦210 billion out of ₦303 billion billed to customers in Q4 2021.

### Solution

This pipeline provides stakeholders with:

- Automated data ingestion and transformation
- Centralized data warehouse for analytics
- Insights into billing efficiency trends
- Customer payment behavior analysis
- Revenue leakage identification

---

## Project Objectives

Successfully extract, transform, and load data from source to data warehouse with the following deliverables:

1. **Automated ETL Pipeline**: Batch processing system for reliable data movement
2. **Pipeline Architecture**: Clear visualization of data workflow and transformations
3. **Analytics-Ready Data**: Structured data warehouse optimized for reporting

---

## Architecture

![alt text](image-1.png)
---

## Dataset

**Source**: [Hugging Face Dataset](https://huggingface.co/datasets/electricsheepafrica/nigerian_energy_and_utilities_billing_payments)

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | String | Unique identifier for each customer |
| `disco` | String | Distribution Company name |
| `billing_month` | String | Month and year of billing cycle |
| `tariff_band` | String | Customer tariff classification |
| `kwh` | Float | Kilowatt-hours consumed |
| `price_ngn_kwh` | Float | Price per kWh in Nigerian Naira |
| `amount_billed_ngn` | Float | Total amount billed |
| `amount_paid_ngn` | Float | Total amount paid by customer |
| `paid_on_time` | Boolean | Payment timeliness indicator |
| `arrears_ngn` | Float | Outstanding arrears amount |

---

## Technologies

| Technology | Purpose |
|-----------|---------|
| **[Python 3.x](https://www.python.org/downloads/)** | Core programming language |
| **[Pandas](https://pandas.pydata.org/)** | Data manipulation and analysis |
| **[SQLAlchemy](https://www.sqlalchemy.org/)** | Database ORM and connection management |
| **[PostgreSQL](https://www.postgresql.org/)** | Data warehouse and analytics database |
| **[python-dotenv](https://pypi.org/project/python-dotenv/)** | Environment variable management |

---

## Getting Started

### Prerequisites

- Python 3.8 or higher
- PostgreSQL 12 or higher
- Git

### Installation

#### 1. Create Project Directory

```bash
mkdir energy_billing
cd energy_billing
````

#### 2. Set Up Virtual Environment

```bash
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate        # Linux/Mac
# OR
.venv\Scripts\activate           # Windows
```

#### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

**requirements.txt**

```txt
python-dotenv==1.0.0
pandas==2.0.0
sqlalchemy==2.0.0
psycopg2-binary==2.9.9
```

#### 4. Configure Environment Variables

Create a `.env` file in the project root:

```ini
# PostgreSQL Connection Configuration
PG_USER=postgres
PG_PASSWORD=yourpassword
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=energydb
```

>  **Security Note**: Add `.env` to `.gitignore` to prevent credential exposure.

#### 5. Create Project Structure

```bash
mkdir -p extract transform load
touch extract/extract.py transform/transform.py load/load.py
```

---

## Project Structure

```
energy_billing/
│
├── extract/
│   └── extract.py           # Data extraction from Hugging Face
│
├── transform/
│   └── transform.py         # Data transformation and cleaning
│
├── load/
│   └── load.py             # Data loading into PostgreSQL
│
├── data/
│   ├── raw/                # Raw parquet files
│   └── processed/          # Transformed CSV files
│
├── .env                    # Environment variables (gitignored)
├── .gitignore
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

---

## Pipeline Implementation

### 1. Extract Stage (`extract/extract.py`)

Fetches raw data from Hugging Face and saves locally.

```python
import pandas as pd
from pathlib import Path

def extract_data(url, output_path):
    """Extract data from Hugging Face dataset"""
    print("Extracting data from source...")

    # Read parquet file from URL
    df = pd.read_parquet(url)

    # Create output directory
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Save raw data
    df.to_parquet(output_path, index=False)
    print(f"✓ Data extracted successfully: {len(df)} records")

    return df

if __name__ == "__main__":
    URL = "https://huggingface.co/datasets/electricsheepafrica/nigerian_energy_and_utilities_billing_payments"
    OUTPUT = "data/raw/billing_data.parquet"
    extract_data(URL, OUTPUT)
```

### 2. Transform Stage (`transform/transform.py`)

Cleans and transforms raw data for analytics.

```python
import pandas as pd
from pathlib import Path

def transform_data(input_path, output_path):
    """Transform and clean billing data"""
    print("Transforming data...")

    # Load raw parquet data
    df = pd.read_parquet(input_path)

    # Split billing_month into year and month
    df[['year', 'month']] = df['billing_month'].str.split('-', expand=True)

    # Reorder columns for better readability
    column_order = [
        'customer_id', 'disco', 'year', 'month', 'tariff_band',
        'kwh', 'price_ngn_kwh', 'amount_billed_ngn',
        'amount_paid_ngn', 'paid_on_time', 'arrears_ngn'
    ]
    df = df[column_order]

    # Data quality checks
    df = df.dropna(subset=['customer_id'])  # Remove records without customer_id
    df['year'] = df['year'].astype(int)
    df['month'] = df['month'].astype(int)

    # Create output directory
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Save transformed data
    df.to_csv(output_path, index=False)
    print(f"✓ Data transformed successfully: {len(df)} records")

    return df

if __name__ == "__main__":
    INPUT = "data/raw/billing_data.parquet"
    OUTPUT = "data/processed/billing_data.csv"
    transform_data(INPUT, OUTPUT)
```

### 3. Load Stage (`load/load.py`)

Loads transformed data into PostgreSQL database.

```python
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def load_data(csv_path, table_name):
    """Load data into PostgreSQL database"""
    print("Loading data into database...")

    # Load environment variables
    load_dotenv()

    # Create database connection string
    db_string = (
        f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
        f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
    )

    # Create SQLAlchemy engine
    engine = create_engine(db_string)

    # Load CSV data
    df = pd.read_csv(csv_path)

    # Load data into PostgreSQL
    df.to_sql(
        table_name,
        engine,
        if_exists='replace',  # Options: 'fail', 'replace', 'append'
        index=False
    )

    print(f"✓ Data loaded successfully: {len(df)} records into '{table_name}' table")

    # Verify load
    with engine.connect() as conn:
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = result.scalar()
        print(f"✓ Verification: {count} records in database")

    return df

if __name__ == "__main__":
    CSV_PATH = "data/processed/billing_data.csv"
    TABLE_NAME = "energy_billing"
    load_data(CSV_PATH, TABLE_NAME)
```

---

## Running the Pipeline

```bash
# Extract data
python extract/extract.py

# Transform data
python transform/transform.py

# Load data
python load/load.py
```

---

## Acknowledgments

- Electric Sheep Africa for providing the dataset
- NERC (Nigerian Electricity Regulatory Commission) for sector reports
- The Nigerian energy sector stakeholders

---

## Support

For questions or support, please open an issue in the GitHub repository or contact [chibuezeanalyst@gmail.com]

---

**Built with ❤️ for the Nigerian Energy Sector**H