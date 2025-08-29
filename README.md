# AWS Cost and Usage Reports Extractor

This Keboola component extracts AWS Cost and Usage Reports (CUR) from S3 in CSV format and loads them into Keboola Storage. The component is built using modern Python technologies including Pydantic for configuration validation, DuckDB for efficient data processing, and a modular architecture for maintainability.

**Table of contents:**  
  
[TOC]

# AWS Setup

First, the CUR report exports need to be set up in the AWS account to be exported to S3 bucket 
in the selected granularity and CSV format. Follow this [guide](https://docs.aws.amazon.com/cur/latest/userguide/cur-create.html)
 to set up the export.
 
 Export Setup:
 
 - Setup S3 bucket
 - Set the report prefix
 - Select granularity
 - Select report versioning (overwrite recommended)
 - Choose GZIP compression type
 
 ![Aws setup](docs/imgs/aws_screen.png)
 

# How It Works

The component operates in several phases:

1. **Configuration Validation**: Uses Pydantic models to validate AWS credentials, S3 bucket settings, and processing options
2. **Report Discovery**: Scans S3 bucket for available Cost and Usage Report manifests based on the configured prefix
3. **Incremental Processing**: Supports incremental loading by tracking the last processed report and timestamp
4. **Data Processing**: Uses DuckDB for efficient bulk loading and processing of large CSV files
5. **Schema Adaptation**: Automatically handles changing report schemas by expanding column sets and normalizing column names for Keboola Storage compatibility
6. **Column Normalization**: Converts AWS column names to Keboola Storage format (e.g., `bill/BillingPeriodEndDate` → `bill__billingPeriodEndDate`)

## Key Features

- **Incremental Loading**: Process only new reports since the last run
- **Schema Evolution**: Handles AWS schema changes automatically
- **Memory Efficient**: Uses DuckDB for processing large datasets without memory issues
- **Error Handling**: Robust error handling with detailed logging
- **Modular Architecture**: Clean separation of concerns across specialized modules

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

- Categories are separated by `__`. e.g.`bill/BillingPeriodEndDate` is converted to `bill__billingPeriodEndDate`
- Any characters that are not alphanumeric or `_` underscores are replaced by underscore. 
E.g. `resourceTags/user:owner` is converted to `resourceTags__user_owner`
- The KBC Storage is case insesitive so the above may lead to duplicate names. In such case the names are deduplicated by adding an index. 
e.g `resourceTags/user:name` and `resourceTags/user:Name` lead to `resourceTags__user_Name` and `resourcetags__user_name_1` 
columns respectively

**Note** That the output schema changes often and may be also affected by the tags and custom columns you define.


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
uv run pytest tests/ -v

# Or with Docker
docker build -t aws-cost-reports-component .
docker run --rm aws-cost-reports-component python -m pytest tests/ -v
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
├── component.py           # Main orchestrator and entry point
├── configuration.py       # Pydantic configuration models
├── aws_report_manager.py  # AWS S3 operations and manifest handling
├── duckdb_processor.py    # DuckDB data processing operations
└── column_normalizer.py   # Column name normalization for KBC Storage
```

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 