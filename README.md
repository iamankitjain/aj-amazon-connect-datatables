# Amazon Connect DataTables Pipeline

A Python-based deployment pipeline for creating and managing Amazon Connect DataTables with attributes and values. This tool automates the creation of data tables, their attributes with validation rules, and populates them with data from JSON configuration files.

## Features

- **Automated Table Creation**: Creates Amazon Connect data tables with configurable properties
- **Attribute Management**: Defines attributes with validation rules, data types, and primary keys
- **Update-First Logic**: Implements true upsert functionality - updates existing records, creates new ones
- **Concurrency Handling**: Automatic retry logic for DATA_TABLE lock level conflicts
- **Batch Operations**: Efficiently processes data using batch APIs with proper error handling
- **List Value Support**: Properly formats TEXT_LIST and NUMBER_LIST attributes
- **Modular Architecture**: Clean, maintainable code structure with separate modules

## Project Structure

```
├── src/
│   ├── config_loader.py      # Configuration file loading utilities
│   ├── table_manager.py      # Data table operations
│   ├── attribute_manager.py  # Attribute creation and management
│   └── value_manager.py      # Value creation with batch processing
├── deploy/
│   ├── connect_datatables_handler.py  # Main deployment handler
│   └── deploy.py             # Local deployment script
├── test/
│   ├── cleanup_pipeline.py   # Cleanup utility
│   └── verify_data.py        # Data verification script
└── config/
    ├── attributes/
    │   ├── CustomerTypes.json    # Attribute definitions for CustomerTypes table
    │   └── PriorityLevels.json   # Attribute definitions for PriorityLevels table
    ├── attribute_values/
    │   ├── CustomerTypes.json    # Sample data for CustomerTypes table
    │   └── PriorityLevels.json   # Sample data for PriorityLevels table
    └── data_tables_config.json  # Main configuration file
```

## Configuration

### Main Configuration (`config/data_tables_config.json`)
```json
{
  "instanceARN": "arn:aws:connect:ca-central-1:123456789012:instance/your-instance-id",
  "dataTables": [
    {
      "name": "CustomerTypes",
      "description": "Customer type lookup table",
      "timeZone": "US/Eastern",
      "valueLockLevel": "DATA_TABLE",
      "tags": {"Environment": "Production"}
    }
  ]
}
```

### Attribute Definitions (`config/attributes/{TableName}.json`)
```json
{
  "tableName": "CustomerTypes",
  "attributes": [
    {
      "name": "CustomerID",
      "valueType": "TEXT",
      "description": "Unique customer identifier",
      "primary": true,
      "validation": {
        "minLength": 1,
        "maxLength": 20
      }
    }
  ]
}
```

### Table Values (`config/attribute_values/{TableName}.json`)
```json
{
  "tableName": "CustomerTypes",
  "values": [
    {
      "primaryValues": [
        {"attributeName": "CustomerID", "value": "CUST001"}
      ],
      "attributes": [
        {"attributeName": "CustomerTier", "value": "Premium"}
      ]
    }
  ]
}
```

## Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/aj-amazon-connect-datatables.git
   cd aj-amazon-connect-datatables
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure AWS credentials**
   ```bash
   aws configure
   ```

## Key Features Explained

### Update-First Deployment
This pipeline implements true **upsert functionality**:
- **Existing records**: Updated with new values
- **New records**: Created automatically
- **Failed updates**: Retried with fresh lock versions
- **Validation errors**: Clearly reported with specific messages

### Lock Version Management
For `DATA_TABLE` lock level:
- Lock versions are automatically refreshed between batches
- Concurrency conflicts are retried with fresh lock versions
- No manual lock version management required

## Usage

### Local Deployment

1. **Update configuration**
   - Edit `config/data_tables_config.json` with your Amazon Connect instance ARN
   - Modify attribute definitions in `config/attributes/` folder
   - Update sample data in `config/attribute_values/` folder

2. **Deploy tables**
   ```bash
   python deploy/deploy.py
   ```

3. **Verify deployment**
   ```bash
   python test/verify_data.py
   ```

4. **Clean up (if needed)**
   ```bash
   python test/cleanup_pipeline.py
   ```



## Supported Data Types

- **TEXT**: String values with length validation
- **NUMBER**: Numeric values with range validation
- **BOOLEAN**: True/false values
- **TEXT_LIST**: Arrays of strings (comma-separated in config)
- **NUMBER_LIST**: Arrays of numbers (comma-separated in config)

## Lock Levels

- **NONE**: No locking (fastest performance, suitable for single-user scenarios)
- **DATA_TABLE**: Table-level locking (concurrency protection, automatic retry on conflicts)
- **PRIMARY_VALUE**: Row-level locking (best balance of performance and concurrency)
- **ATTRIBUTE**: Attribute-level locking (fine-grained control)
- **VALUE**: Individual value locking (maximum granularity)

## Key Capabilities

### Update-First Logic
The pipeline implements intelligent upsert functionality:
1. **Phase 1**: Attempts to update all records
2. **Phase 2**: Creates records that failed with "Value not found"
3. **Retry Logic**: Automatically retries concurrency conflicts with fresh lock versions

### Sample Tables
The repository includes two example tables:

1. **CustomerTypes**: Simple table with single primary key (20 records)
2. **PriorityLevels**: Complex table with composite primary keys (20 records)

### Performance
- **NONE lock level**: ~40-80 API calls for 1000 entries (fastest)
- **DATA_TABLE lock level**: ~120+ API calls for 1000 entries (with concurrency protection)

## Troubleshooting

### Common Issues

1. **Validation errors**: Check that data values meet attribute validation rules (e.g., `multipleOf`, `minimum`)
2. **List format errors**: Use comma-separated strings in JSON, not arrays
3. **Permission errors**: Verify AWS credentials have Amazon Connect permissions
4. **Lock conflicts**: Automatic retry handles most concurrency issues with DATA_TABLE lock level

### Debug Information

The deployment script provides detailed console output including:
- Phase-by-phase processing (Update → Create)
- Batch-level success/failure counts
- Specific error messages for failed operations
- Lock version refresh notifications
- Final summary with totals

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
- Create an issue in the GitHub repository
- Check the troubleshooting section
- Review AWS Amazon Connect documentation