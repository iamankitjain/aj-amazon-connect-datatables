#!/usr/bin/env python3
"""
Cleanup utility for Amazon Connect DataTables.

This script deletes all data tables created by the deployment pipeline.
Use this for testing or when you need to completely reset your data tables.
"""

import json
import boto3
from botocore.exceptions import ClientError


class CleanupError(Exception):
    """Custom exception for cleanup operations."""
    pass


def _find_table_id(connect, instance_arn, table_name):
    """Find table ID by name."""
    response = connect.list_data_tables(InstanceId=instance_arn, MaxResults=100)
    for table in response.get('DataTableSummaryList', []):
        if table['Name'] == table_name:
            return table['Id']
    return None


def _delete_single_table(connect, instance_arn, table_name):
    """Delete a single data table."""
    try:
        table_id = _find_table_id(connect, instance_arn, table_name)
        
        if not table_id:
            return {'name': table_name, 'status': 'not_found', 'message': 'Table does not exist'}
        
        connect.delete_data_table(InstanceId=instance_arn, DataTableId=table_id)
        return {'name': table_name, 'status': 'deleted', 'tableId': table_id}
        
    except ClientError as e:
        return {'name': table_name, 'status': 'failed', 'error': str(e)}


def cleanup_data_tables():
    """
    Delete all data tables created by the deployment pipeline.
    
    Reads the configuration file to identify which tables to delete,
    then removes them from the Amazon Connect instance.
    
    Returns:
        list: Results for each table deletion attempt
    """
    try:
        with open('config/data_tables_config.json', 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError("Configuration file 'config/data_tables_config.json' not found")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file: {str(e)}")
    except IOError as e:
        raise IOError(f"Cannot read configuration file: {str(e)}")
    
    try:
        connect = boto3.client('connect', region_name='ca-central-1')
        results = []
        
        for table_config in config['dataTables']:
            result = _delete_single_table(connect, config['instanceARN'], table_config['name'])
            results.append(result)
        
        return results
        
    except Exception as e:
        raise CleanupError(f"Error during cleanup process: {str(e)}")


def main():
    """
    Main function to execute the cleanup process and display results.
    """
    print("Cleaning up Amazon Connect Data Tables...")
    print("=" * 50)

    try:
        # Execute cleanup and get results
        results = cleanup_data_tables()
        
        # Display results with status icons
        for result in results:
            status_icons = {'deleted': '[OK]', 'not_found': '[SKIP]'}
            status_icon = status_icons.get(result['status'], '[FAIL]')
            print(f"{status_icon} {result['name']}: {result['status']}")

            if 'message' in result:
                print(f"  - {result['message']}")
            if 'error' in result:
                print(f"  - Error: {result['error']}")
            if 'tableId' in result:
                print(f"  - Table ID: {result['tableId']}")
                
    except FileNotFoundError as e:
        print(f"[FAIL] Configuration file error: {str(e)}")
    except ValueError as e:
        print(f"[FAIL] Configuration format error: {str(e)}")
    except IOError as e:
        print(f"[FAIL] File access error: {str(e)}")
    except CleanupError as e:
        print(f"[FAIL] Cleanup process error: {str(e)}")
    except Exception as e:
        print(f"[FAIL] Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()
