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

![Pipeline Architecture](assests/image-1.png)

The pipeline follows a three-stage ETL process:

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│                 │       │                 │       │                 │
│    EXTRACT      │──────▶│   TRANSFORM     │──────▶│      LOAD       │
│                 │       │                 │       │                 │
│  Hugging Face   │       │  Data Cleaning  │       │   PostgreSQL    │
│   (Parquet)     │       │  Feature Eng.   │       │   (energydb)    │
│                 │       │  Validation     │       │                 │
└─────────────────┘       └─────────────────┘       └─────────────────┘
```

---

## Dataset

**Source**: [Hugging Face Dataset](https://huggingface.co/datasets/electricsheepafrica/nigerian_energy_and_utilities_billing_payments)

### Schema

| Column              | Type    | Description                         |
| ------------------- | ------- | ----------------------------------- |
| `customer_id`       | String  | Unique identifier for each customer |
| `disco`             | String  | Distribution Company name           |
| `billing_month`     | String  | Month and year of billing cycle     |
| `tariff_band`       | String  | Customer tariff classification      |
| `kwh`               | Float   | Kilowatt-hours consumed             |
| `price_ngn_kwh`     | Float   | Price per kWh in Nigerian Naira     |
| `amount_billed_ngn` | Float   | Total amount billed                 |
| `amount_paid_ngn`   | Float   | Total amount paid by customer       |
| `paid_on_time`      | Boolean | Payment timeliness indicator        |
| `arrears_ngn`       | Float   | Outstanding arrears amount          |

---

## Technologies

| Technology                                                   | Purpose                                |
| ------------------------------------------------------------ | -------------------------------------- |
| **[Python 3.8](https://www.python.org/downloads/)**          | Core programming language              |
| **[Pandas](https://pandas.pydata.org/)**                     | Data manipulation and analysis         |
| **[SQLAlchemy](https://www.sqlalchemy.org/)**                | Database ORM and connection management |
| **[PostgreSQL 18](https://www.postgresql.org/)**             | Data warehouse and analytics database  |
| **[python-dotenv](https://pypi.org/project/python-dotenv/)** | Environment variable management        |
| **[Datasets](https://huggingface.co/docs/datasets/)**        | Hugging Face dataset library           |

---

## Getting Started

### Prerequisites

Before you begin, ensure you have the following installed:

- Python 3.8 or higher
- PostgreSQL 18
- Git
- pip (Python package manager)

### Installation

#### Step 1: Create Project Directory

```bash
mkdir energy_billing
cd energy_billing
```

#### Step 2: Set Up Virtual Environment

```bash
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate        # Linux/Mac
# OR
.venv\Scripts\activate           # Windows
```

#### Step 3: Install Dependencies

Create a `requirements.txt` file:

```txt
pandas==2.2.3
datasets==2.19.0
sqlalchemy==2.0.36
python-dotenv==1.0.1
psycopg2-binary==2.9.9
```

Install the dependencies:

```bash
pip install -r requirements.txt
```

#### Step 4: Configure Environment Variables

Create a `.env` file in the project root:

```ini
# PostgreSQL Connection Configuration
PG_USER=postgres
PG_PASSWORD=yourpassword
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=energydb

# Hugging Face Dataset URL
```API_URL=https://huggingface.co/datasets/electricsheepafrica/nigerian_energy_and_utilities_billing_payments/resolve/main/nigerian_energy_and_utilities_billing_payments.parquet
```

> **Security Note**: Add `.env` to `.gitignore` to prevent credential exposure.

#### Step 5: Create Project Structure

```bash
# Create main pipeline directory
mkdir -p etl_pipeline

# Create data directories
mkdir -p data/raw data/processed

# Create pipeline scripts
touch etl_pipeline/extract.py
touch etl_pipeline/transform.py
touch etl_pipeline/load.py
```

---

## Project Structure

```
energy_billing/
│
├── etl_pipeline/
│   ├── __init__.py
│   ├── extract.py              # Data extraction from Hugging Face
│   ├── transform.py            # Data transformation and cleaning
│   └── load.py                 # Data loading into PostgreSQL
│
├── data/
│   ├── raw/                    # Raw parquet files
│   └── processed/              # Transformed CSV files
│
├── assests/
│   └── image-1.png            # Architecture diagram
│
├── .env                        # Environment variables (gitignored)
├── .gitignore
└── README.md                   # This file
```

---

## Pipeline Implementation

### 1. Extract Stage (`etl_pipeline/extract.py`)

Fetches raw data from Hugging Face and saves locally.

### 2. Transform Stage (`etl_pipeline/transform.py`)

Cleans and transforms raw data for analytics.

### 3. Load Stage (`etl_pipeline/load.py`)

## Loads transformed data into PostgreSQL database.

## Running the Pipeline

Execute each stage separately for testing or debugging:

```bash
# Stage 1: Extract data from Hugging Face
python etl_pipeline/extract.py

# Stage 2: Transform and clean data
python etl_pipeline/transform.py

# Stage 3: Load data into PostgreSQL
python etl_pipeline/load.py
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## Acknowledgments

- **Electric Sheep Africa** for providing the comprehensive dataset
- **NERC (Nigerian Electricity Regulatory Commission)** for sector reports and guidance
- **The Nigerian energy sector stakeholders** for their continuous efforts to improve the sector

---

## Support

For questions, issues, or support:

- Email: [chibuezeanalyst@gmail.com](mailto:chibuezeanalyst@gmail.com)

---

## Roadmap

Future enhancements planned for this project:

- [ ] Add data validation tests
- [ ] Implement incremental loading
- [ ] Create interactive dashboard with Plotly/Streamlit
- [ ] Add automated scheduling with Apache Airflow
- [ ] Implement data quality monitoring
- [ ] Add API endpoint for real-time queries
- [ ] Create Docker container for easy deployment
- [ ] Add unit and integration tests

---

## Additional Resources

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Pandas User Guide](https://pandas.pydata.org/docs/user_guide/index.html)
- [SQLAlchemy Tutorial](https://docs.sqlalchemy.org/en/20/tutorial/)
- [Python Dotenv Documentation](https://pypi.org/project/python-dotenv/)
- [NERC Reports](https://nerc.gov.ng/)

---

**Built with ❤️ for the Nigerian Energy Sector**
