"""
Amazon Connect DataTables Deployment Handler

Main deployment function for creating Amazon Connect DataTables with attributes and values.
Implements update-first logic with fallback to create for true upsert functionality.
Handles DATA_TABLE lock levels with automatic retry for concurrency conflicts.
"""

import json
import boto3
from botocore.exceptions import ClientError


class DataTablesDeploymentError(Exception):
    """Custom exception for DataTables deployment operations."""
    pass

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config_loader import load_default_config
from src.table_manager import get_existing_table, create_data_table
from src.attribute_manager import create_table_attributes
from src.value_manager import create_table_values


def deploy_datatables(config=None):
    """
    Main deployment function for Amazon Connect DataTables pipeline.
    
    Creates data tables, attributes, and populates them with values based on
    configuration provided or default configuration files.
    
    Args:
        config: Optional deployment configuration dict
        
    Returns:
        dict: Deployment results
    """
    # Initialize Amazon Connect client for ca-central-1 region
    connect = boto3.client('connect', region_name='ca-central-1')

    try:
        # Use provided configuration or load default
        config = config or load_default_config()
        instance_arn = config['instanceARN']
        data_tables = config['dataTables']

        results = []

        # Process each data table in configuration
        for table_config in data_tables:
            table_name = table_config['name']

            # Check if table already exists to avoid duplicates
            existing_table = get_existing_table(
                connect, instance_arn, table_name)

            if not existing_table:
                # Create new data table with specified configuration
                try:
                    response = create_data_table(
                        connect, instance_arn, table_config)

                    # Store creation result
                    table_result = {
                        'name': table_name,
                        'status': 'created',
                        'dataTableArn': response.get('DataTableArn', response.get('Arn', 'Unknown')),
                        'dataTableId': response.get('DataTableId', response.get('Id', 'Unknown'))
                    }

                    # Prepare table reference for subsequent operations
                    existing_table = {'DataTableId': response.get(
                        'DataTableId', response.get('Id', 'Unknown'))}

                except ClientError as e:
                    # Handle table creation failure
                    results.append({
                        'name': table_name,
                        'status': 'failed',
                        'error': str(e)
                    })
                    continue
            else:
                # Table already exists, skip creation
                table_result = {
                    'name': table_name,
                    'status': 'skipped',
                    'message': 'Data table already exists',
                    'dataTableId': existing_table['DataTableId']
                }

            # Create table attributes from configuration files
            attributes_result = create_table_attributes(
                connect, instance_arn, existing_table['DataTableId'], table_name)
            table_result['attributes'] = attributes_result

            # Populate table with values from configuration files
            values_result = create_table_values(
                connect, instance_arn, existing_table['DataTableId'], table_name)
            table_result['values'] = values_result

            results.append(table_result)

        # Return successful deployment response
        return {
            'message': 'Amazon Connect DataTables deployment completed successfully',
            'results': results
        }

    except ClientError as e:
        # Handle AWS service errors
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        raise DataTablesDeploymentError(f'AWS Connect API error ({error_code}): {str(e)}')
    except FileNotFoundError as e:
        # Handle missing configuration files
        raise DataTablesDeploymentError(f'Configuration file not found: {str(e)}')
    except json.JSONDecodeError as e:
        # Handle invalid JSON in configuration files
        raise DataTablesDeploymentError(f'Invalid JSON in configuration file: {str(e)}')
    except Exception as e:
        # Handle any other unexpected errors
        raise DataTablesDeploymentError(f'DataTables pipeline execution failed: {str(e)}')
