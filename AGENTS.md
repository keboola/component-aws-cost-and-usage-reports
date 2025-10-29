# AI Agents Guide

This document provides context and guidance for AI agents working with the AWS Cost and Usage Reports Extractor component.

## Project Overview

**Purpose**: Keboola component that extracts AWS Cost and Usage Reports (CUR) from S3 and loads them into Keboola Storage.

**Key Features**:
- Automatic detection and support for both CUR 1.0 (legacy) and CUR 2.0 (modern) formats
- Handler-based architecture with factory pattern for extensibility
- Parallel processing of ZIP/GZIP files for optimal performance
- DuckDB-powered data processing for memory efficiency
- Column normalization for Keboola Storage compatibility

## Architecture

### Design Patterns
- **Handler Pattern**: Separate handlers for each CUR version with shared interface
- **Factory Pattern**: Automatic handler selection based on detected CUR version
- **Strategy Pattern**: Version-specific logic encapsulated in handlers
- **Composition over Inheritance**: Component uses handlers rather than extending them

### Core Components

```
src/
├── component.py                     # Main orchestrator and entry point
├── configuration.py                 # Pydantic configuration models  
├── duckdb_client.py                 # DuckDB data processing operations
└── aws_report_handlers/             # Modular handler architecture
    ├── __init__.py                  # Package initialization
    ├── base_handler.py              # Abstract base class for handlers
    ├── version_detector.py          # CUR version detection logic
    ├── handler_factory.py           # Factory for creating handlers
    ├── cur_1_report_handler.py      # CUR 1.0 (legacy) format handler
    └── cur_2_report_handler.py      # CUR 2.0 (modern) format handler
```

## CUR Format Differences

### CUR 1.0 (Legacy)
- **Files**: ZIP-compressed CSV files
- **S3 Structure**: Date-based folders (`YYYYMMDD-YYYYMMDD`)
- **Manifest Location**: Root of date folders (`ReportName-Manifest.json`)
- **Manifest Key**: `reportKeys` with relative paths
- **Column Format**: Category-based (`bill/InvoiceId`, `lineItem/UsageAmount`)
- **Detection**: Date patterns, ZIP files, lack of `BILLING_PERIOD=`

### CUR 2.0 (Modern)
- **Files**: GZIP-compressed CSV files  
- **S3 Structure**: `BILLING_PERIOD=YYYY-MM` partitions
- **Manifest Location**: `metadata/BILLING_PERIOD=YYYY-MM/` subfolders
- **Manifest Key**: `dataFiles` with full S3 URLs
- **Column Format**: Flat naming (`bill_invoice_id`, `line_item_usage_amount`)
- **Detection**: `BILLING_PERIOD=` in S3 paths (primary indicator)

## Development Guidelines

### Adding New CUR Version Support
1. Create new handler in `aws_report_handlers/` extending `BaseReportHandler`
2. Implement all abstract methods: `retrieve_manifests`, `get_csv_patterns`, `normalize_columns`, `filter_by_date_range`
3. Update `ReportVersionDetector.detect_version()` to recognize new version
4. Update `ReportHandlerFactory.create_handler()` to instantiate new handler
5. Add test data to `tests/mock_data/` with S3 structure and manifests
6. Update `mock_s3_test_runner.py` to test new version

### Code Quality Standards
- **Type Hints**: All functions must have complete type annotations
- **Error Handling**: Graceful degradation with informative logging
- **Testing**: Mock S3 tests for all handlers (`tests/mock_s3_test_runner.py`)
- **Linting**: Code must pass `flake8` with project configuration
- **Documentation**: All classes and public methods need docstrings

### Testing Strategy
- **Mock S3 Tests**: Use `MockS3Client` for local testing without AWS
- **Test Data**: Realistic mock manifests and CSV files for both CUR versions
- **Functional Tests**: End-to-end tests with component and DuckDB
- **Docker Tests**: Verify functionality in containerized environment

## Configuration

### Required Parameters
```json
{
  "aws_parameters": {
    "api_key_id": "AWS_ACCESS_KEY_ID",
    "#api_key_secret": "AWS_SECRET_ACCESS_KEY", 
    "aws_region": "eu-central-1",
    "s3_bucket": "my-cur-bucket"
  },
  "report_path_prefix": "reports/billing/cur",
  "loading_options": {
    "incremental_output": 0,
    "pkey": []
  }
}
```

### Key Configuration Notes
- `report_path_prefix`: S3 prefix where CUR reports are stored
- `since_last`: Enables incremental processing (default: false)
- `min_date_since`/`max_date`: Date range filtering
- All dates support relative formats like "30 days ago", "yesterday"

## Common Tasks

### Running Tests
```bash
# Local testing
uv run python tests/mock_s3_test_runner.py

# Docker testing  
docker build -t aws-cur-component .
docker run --rm aws-cur-component python /code/tests/mock_s3_test_runner.py
```

### Debugging CUR Version Detection
```python
from aws_report_handlers import ReportVersionDetector

# Check version detection logic
version = ReportVersionDetector.detect_version(s3_objects)
details = ReportVersionDetector.get_version_details(s3_objects)
```

### Adding New Column Normalization Rules
Modify the `normalize_columns` method in the appropriate handler:
- CUR 1.0: Category/name format → `category__name`
- CUR 2.0: Usually already normalized, but handle edge cases

## Dependencies

### Core Runtime
- **Python**: 3.13+
- **DuckDB**: High-performance analytics database  
- **Boto3**: AWS SDK for S3 operations
- **Pydantic**: Configuration validation
- **Keboola Component**: Base framework and utilities

### Development
- **UV**: Package management and dependency resolution
- **Flake8**: Code linting and style enforcement
- **Docker**: Containerization and testing

## Memory and Performance

### DuckDB Optimizations
- Uses memory-mapped files for large datasets
- Parallel CSV processing with configurable thread count
- Automatic schema detection and type inference
- Bulk loading for optimal performance

### File Processing
- **CUR 1.0**: Parallel ZIP extraction using ThreadPoolExecutor
- **CUR 2.0**: Sequential GZIP extraction (files typically smaller)
- Temporary file cleanup after processing
- Progress logging for long-running operations

## Troubleshooting

### Common Issues
1. **Region Mismatch**: Ensure `aws_region` matches S3 bucket region
2. **Permission Issues**: S3 credentials need ListObjects and GetObject permissions
3. **Memory Issues**: Adjust DuckDB `max_memory` setting for large datasets
4. **Date Filtering**: Check date format and timezone handling

### Debugging Tips
- Enable debug logging for detailed execution flow
- Check S3 object listing to verify report structure
- Examine manifest content for format validation
- Monitor temporary file creation and cleanup

## Data Flow

1. **Configuration Validation** → Pydantic models validate all parameters
2. **Handler Creation** → Factory detects CUR version and creates appropriate handler  
3. **S3 Discovery** → Handler lists S3 objects matching report prefix
4. **Manifest Processing** → Version-specific manifest parsing and metadata extraction
5. **File Processing** → ZIP/GZIP extraction to temporary directories
6. **DuckDB Loading** → Bulk load all CSV files with schema unification
7. **Column Normalization** → Keboola Storage compatible column names
8. **Data Export** → Write final CSV with manifest metadata
9. **State Management** → Save execution state for incremental processing

## Extension Points

### Custom Handlers
Extend `BaseReportHandler` for new CUR versions or custom formats:
```python
class CustomReportHandler(BaseReportHandler):
    def __init__(self, s3_client, bucket: str, report_prefix: str):
        super().__init__(s3_client, bucket, report_prefix)
    
    def retrieve_manifests(self, s3_objects, report_name):
        # Custom manifest parsing logic
        pass
    
    # Implement other abstract methods...
```

### Custom Version Detection
Add new detection logic to `ReportVersionDetector.detect_version()`:
```python
# Add new version indicators
if any("CUSTOM_PATTERN" in obj["Key"] for obj in s3_objects):
    return "custom"
```

This guide should provide sufficient context for AI agents to effectively understand and work with this codebase.
