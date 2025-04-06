# ETL, Reporting & PDF Export Pipeline

A professional ETL (Extract, Transform, Load) pipeline for processing sales data, generating reports, and exporting to PDF.

## Features

- **Modular Architecture**: Clean separation of concerns with dedicated modules for extraction, transformation, loading, and reporting
- **Multiple Database Support**: Works with both SQLite and PostgreSQL databases
- **Data Warehouse Tables**: Stores aggregated data in fact tables for efficient querying
- **Automated Report Generation**: Creates CSV reports and professional PDF reports with visualizations
- **Robust Error Handling**: Comprehensive logging and error management
- **Configuration Management**: Flexible configuration via environment variables
- **API Integration**: Fetches real-time exchange rates from external API with fallback mechanism

## Project Structure

```
etl_pipeline/
├── src/                            # Source code directory
│   ├── etl_pipeline/               # Main package
│   │   ├── config/                 # Configuration management
│   │   │   ├── __init__.py
│   │   │   └── settings.py         # Configuration loading and management
│   │   ├── extractors/             # Data extraction modules
│   │   │   ├── __init__.py
│   │   │   ├── csv_extractor.py    # CSV file extraction
│   │   │   └── exchange_rate_extractor.py  # Exchange rate API extraction
│   │   ├── transformers/           # Data transformation modules
│   │   │   ├── __init__.py
│   │   │   └── sales_transformer.py  # Sales data transformation
│   │   ├── loaders/                # Data loading modules
│   │   │   ├── __init__.py
│   │   │   └── database_loader.py  # Database loading
│   │   ├── reports/                # Report generation modules
│   │   │   ├── __init__.py
│   │   │   └── report_generator.py # Report and PDF generation
│   │   ├── utils/                  # Utility modules
│   │   │   ├── __init__.py
│   │   │   ├── database.py         # Database utilities
│   │   │   └── logging_config.py   # Logging configuration
│   │   ├── models/                 # Data models (for future use)
│   │   │   └── __init__.py
│   │   └── __init__.py
│   └── main.py                     # Main entry point
├── reports/                        # Generated reports directory
│   ├── affiliate_sales.csv
│   ├── category_sales.csv
│   ├── monthly_sales.csv
│   └── sales_report.pdf
├── .env                            # Environment variables
├── .env.example                    # Example environment variables
├── setup.py                        # Package setup script
├── requirements.txt                # Dependencies
├── docker-compose.yml              # Docker Compose configuration
└── README.md                       # Project documentation
```

## Installation

### Local Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/etl-pipeline.git
   cd etl-pipeline
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the package in development mode:
   ```bash
   pip install -e .
   ```

### Docker Installation

1. Build and start the containers:
   ```bash
   docker-compose up -d
   ```

## Configuration

Create a `.env` file based on `.env.example` with your configuration:

```
# Database Configuration
# Set DB_TYPE to 'sqlite' or 'postgresql'
DB_TYPE=postgresql

# SQLite Configuration
SQLITE_DB_PATH=sales_database.sqlite

# PostgreSQL Configuration
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=sales
PG_USER=postgres
PG_PASSWORD=password

# Reports Directory
REPORTS_DIR=reports

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=etl_process.log
```

## Usage

### Running the ETL Pipeline

```bash
# Run the ETL pipeline
python src/main.py

# Or use the installed entry point
etl-pipeline
```

### Docker Usage

```bash
# Run the ETL pipeline in Docker
docker-compose up etl
```

## Data Warehouse Schema

### Fact Tables

- **fact_affiliate_sales**: Aggregated sales by affiliate
  - affiliate_name (TEXT, PK)
  - total_sales_usd (REAL)
  - last_updated (TIMESTAMP)

- **fact_category_sales**: Aggregated sales by category
  - category (TEXT, PK)
  - total_sales_usd (REAL)
  - last_updated (TIMESTAMP)

- **fact_monthly_sales**: Aggregated sales by month
  - month (TEXT, PK)
  - total_sales_usd (REAL)
  - last_updated (TIMESTAMP)

### Raw Data Tables

- **sales**: Raw sales data
  - order_id (INTEGER, PK)
  - affiliate_name (TEXT)
  - sales_amount (REAL)
  - currency (TEXT)
  - order_date (DATE)
  - category (TEXT)
  - sales_amount_usd (REAL)
  - month (TEXT)

- **exchange_rates**: Exchange rate data
  - currency (TEXT, PK)
  - rate (REAL)
  - updated_at (TIMESTAMP)

## Development

### Adding New Features

1. Create a new module in the appropriate directory
2. Update the main entry point to use the new module
3. Add tests for the new module
4. Update documentation

### Running Tests

```bash
# Run tests
pytest
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
