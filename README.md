# AWS Cost and Usage Reports Extractor

This Keboola component extracts AWS Cost and Usage Reports (CUR) from S3 in CSV format and loads them into Keboola Storage. The component supports both **CUR 1.0 (legacy)** and **CUR 2.0 (modern)** formats automatically, providing seamless migration and compatibility.

Built using modern Python technologies including Pydantic for configuration validation, DuckDB for efficient data processing, and a modular handler architecture for maintainability and extensibility.

**Table of contents:**  
  
[TOC]

# AWS Setup

First, the CUR report exports need to be set up in the AWS account to be exported to S3 bucket 
in the selected granularity and CSV format. Follow this [guide](https://docs.aws.amazon.com/cur/latest/userguide/cur-create.html)
 to set up the export.

## Supported Formats

The component automatically detects and supports both CUR formats:

### CUR 1.0 (Legacy Format)
- **File format**: ZIP-compressed CSV files
- **S3 structure**: Date-based folders (`YYYYMMDD-YYYYMMDD`)
- **Manifest location**: Root of date folders
- **Column format**: Category-based (`bill/InvoiceId`, `lineItem/UsageAmount`)

### CUR 2.0 (Modern Format) 
- **File format**: GZIP-compressed CSV files
- **S3 structure**: `BILLING_PERIOD=YYYY-MM` partitions
- **Manifest location**: `metadata/BILLING_PERIOD=YYYY-MM/` subfolders
- **Column format**: Flat naming (`bill_invoice_id`, `line_item_usage_amount`)

## Export Setup

 - Setup S3 bucket
 - Set the report prefix
 - Select granularity (Daily, Monthly, etc.)
 - Select report versioning (overwrite recommended)
 - Choose GZIP compression type (required for CUR 2.0)
 
 ![Aws setup](docs/imgs/aws_screen.png)
 

# How It Works

The component uses a modular handler architecture that automatically detects the CUR format and processes data accordingly:

1. **Configuration Validation**: Uses Pydantic models to validate AWS credentials, S3 bucket settings, and processing options
2. **Version Detection**: Automatically detects CUR 1.0 vs CUR 2.0 based on S3 object structure (`BILLING_PERIOD=` indicates CUR 2.0)
3. **Handler Selection**: Creates appropriate handler (CUR1ReportHandler or CUR2ReportHandler) via factory pattern
4. **Report Discovery**: Scans S3 bucket for available Cost and Usage Report manifests using format-specific logic
5. **File Processing**: 
   - **CUR 1.0**: Downloads and extracts ZIP files in parallel to temporary directories
   - **CUR 2.0**: Downloads and extracts GZIP files for local processing
6. **Data Loading**: Uses DuckDB for efficient bulk loading of extracted CSV files
7. **Schema Adaptation**: Automatically handles different column formats and normalizes them for Keboola Storage
8. **Incremental Processing**: Supports incremental loading by tracking the last processed report and timestamp

## Key Features

- **Automatic Version Detection**: Seamlessly works with both CUR 1.0 and CUR 2.0 without configuration changes
- **Parallel Processing**: ZIP/GZIP extraction runs in parallel for optimal performance
- **Incremental Loading**: Process only new reports since the last run
- **Schema Evolution**: Handles AWS schema changes automatically across both formats
- **Memory Efficient**: Uses DuckDB for processing large datasets without memory issues
- **Error Handling**: Robust error handling with detailed logging
- **Handler Architecture**: Clean separation of format-specific logic with shared interfaces

# Configuration

The component configuration is validated using Pydantic models for type safety and error handling.

## AWS Parameters

- **`api_key_id`**: AWS Access Key ID with S3 read permissions
- **`api_key_secret`**: AWS Secret Access Key 
- **`aws_region`**: AWS region where your S3 bucket is located (default: `eu-central-1`)
- **`s3_bucket`**: S3 bucket name containing the Cost and Usage Reports

## Report Configuration

- **`report_path_prefix`**: The prefix path to your reports in S3 bucket (e.g., `my-company-cur-reports` or `reports/billing/cur`)
- **`min_date_since`**: Minimum date to start processing reports from (YYYY-MM-DD format or relative like "30 days ago"). Optional.
- **`max_date`**: Maximum date to process reports until (YYYY-MM-DD format or relative like "yesterday"). Default: "now"
- **`since_last`**: Enable incremental loading - process only reports newer than the last run (default: `true`)

## Loading Options

- **`incremental_output`**: Set to 1 for incremental loading, 0 for full reload
- **`pkey`**: Primary key columns for incremental loading (array of column names)

## Additional Options

- **`debug`**: Enable debug logging for troubleshooting (default: `false`)

# Output

The output schema is described [here](https://docs.aws.amazon.com/cur/latest/userguide/data-dictionary.html)


**IMPORTANT NOTE** The result column names are modified to match the KBC Storage column name requirements:

## Column Name Normalization

The component handles different column formats from both CUR versions:

### CUR 1.0 (Legacy) Columns
- **Original format**: Category-based like `bill/BillingPeriodEndDate`, `lineItem/UsageAmount`
- **Converted to**: `bill__BillingPeriodEndDate`, `lineItem__UsageAmount`

### CUR 2.0 (Modern) Columns  
- **Original format**: Flat naming like `bill_billing_period_end_date`, `line_item_usage_amount`
- **Used as-is**: Column names are already Keboola Storage compatible

### General Rules
- Categories are separated by `__` (double underscore)
- Any characters that are not alphanumeric or `_` underscores are replaced by underscore
- E.g. `resourceTags/user:owner` is converted to `resourceTags__user_owner`
- KBC Storage is case insensitive so duplicate names are deduplicated by adding an index
- E.g. `resourceTags/user:name` and `resourceTags/user:Name` become `resourceTags__user_Name` and `resourcetags__user_name_1`

**Note** The output schema changes often and may be affected by the CUR version, tags, and custom columns you define.


# Development

This component uses modern Python development tools and practices:

- **UV**: Fast Python package installer and resolver for dependency management
- **Pydantic**: Type-safe configuration validation
- **DuckDB**: High-performance analytics database for data processing
- **Pytest**: Testing framework with comprehensive test coverage
- **Flake8**: Code linting and style enforcement

## Local Development

1. **Clone and setup:**
```bash
git clone <repository-url>
cd component-aws-cost-and-usage-reports
```

2. **Build and run with Docker:**
```bash
docker build -t aws-cost-reports-component .
docker run --rm -v $(pwd)/data:/data aws-cost-reports-component
```

3. **Run tests:**
```bash
# Local testing with UV
uv sync --extra test
uv run python tests/mock_s3_test_runner.py

# Or with Docker
docker build -t aws-cost-reports-component .
docker run --rm aws-cost-reports-component python /code/tests/mock_s3_test_runner.py
```

4. **Code quality checks:**
```bash
# Linting
uv run flake8 src/ --config=flake8.cfg

# Or with Docker
docker run --rm aws-cost-reports-component python -m flake8 src/ --config=flake8.cfg
```

## Project Structure

```
src/
├── component.py                     # Main orchestrator and entry point
├── configuration.py                 # Pydantic configuration models  
├── duckdb_client.py                 # DuckDB data processing operations
└── aws_report_handlers/             # Modular handler architecture
    ├── __init__.py                  # Package initialization
    ├── base_handler.py              # Abstract base class for handlers
    ├── version_detector.py           # CUR version detection logic
    ├── handler_factory.py           # Factory for creating handlers
    ├── cur_1_report_handler.py      # CUR 1.0 (legacy) format handler
    └── cur_2_report_handler.py      # CUR 2.0 (modern) format handler

tests/
├── mock_s3_test_runner.py           # Comprehensive mock S3 tests
└── mock_data/                       # Test data for both CUR formats
    ├── cur_1_0/                     # CUR 1.0 test data and structure
    └── cur_2_0/                     # CUR 2.0 test data and structure
```

### Architecture Overview

- **Handler Pattern**: Separate handlers for each CUR format with shared interface
- **Factory Pattern**: Automatic handler selection based on detected format  
- **Strategy Pattern**: Format-specific logic encapsulated in handlers
- **Composition**: Component uses handlers rather than inheritance

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 