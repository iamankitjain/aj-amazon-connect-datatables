#!/usr/bin/env python3
"""
Data verification utility for Amazon Connect DataTables.

This script verifies that data tables and their values were created correctly
by querying the actual data from Amazon Connect and displaying it.
"""

import json
import boto3
from botocore.exceptions import ClientError

def _display_table_attributes(connect, instance_arn, table_id):
    """Display table attributes and return primary keys."""
    try:
        attr_response = connect.list_data_table_attributes(
            InstanceId=instance_arn, DataTableId=table_id, MaxResults=100)
        
        attributes = attr_response.get('Attributes', [])
        print(f"   Attributes ({len(attributes)}):")
        
        primary_keys = []
        for attr in attributes:
            is_primary = attr.get('Primary', False)
            if is_primary:
                primary_keys.append(attr['Name'])
            print(f"      - {attr['Name']} ({attr['ValueType']}) {'[PRIMARY]' if is_primary else ''}")
        
        print(f"   Primary Keys: {primary_keys}")
        return primary_keys
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"   Attributes: AWS API Error ({error_code}) - {str(e)}")
        return []
    except Exception as e:
        print(f"   Attributes: Unexpected error - {str(e)}")
        return []


def _display_table_values(connect, instance_arn, table_id):
    """Display sample table values."""
    try:
        values_response = connect.list_data_table_values(
            InstanceId=instance_arn, DataTableId=table_id, MaxResults=5)
        
        values = values_response.get('Values', [])
        print(f"   Values: {len(values)} found (showing first 3)")
        
        for i, value in enumerate(values[:3]):
            print(f"      Row {i+1}:")
            
    except ClientError as e:
        print(f"   Values: Error listing values - {e}")


def _display_single_table(connect, instance_arn, table):
    """Display information for a single table."""
    table_name = table['Name']
    table_id = table['Id']
    
    print(f"\nTable: {table_name}")
    print(f"   ID: {table_id}")
    
    _display_table_attributes(connect, instance_arn, table_id)
    _display_table_values(connect, instance_arn, table_id)


def verify_table_data():
    """
    Verify what data actually exists in Amazon Connect DataTables.
    
    Queries the Amazon Connect instance to list all data tables,
    their attributes, and sample values to verify deployment success.
    
    Displays:
    - Table names and IDs
    - Attribute definitions and types
    - Primary key information
    - Sample data values
    """
    try:
        with open('config/data_tables_config.json', 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print("Error: Configuration file 'config/data_tables_config.json' not found")
        return
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in configuration file - {str(e)}")
        return
    except IOError as e:
        print(f"Error: Cannot read configuration file - {str(e)}")
        return
    
    try:
        connect = boto3.client('connect', region_name='ca-central-1')
        
        response = connect.list_data_tables(
            InstanceId=config['instanceARN'], MaxResults=100)
        
        print("Data Tables Found:")
        print("=" * 50)
        
        for table in response.get('DataTableSummaryList', []):
            _display_single_table(connect, config['instanceARN'], table)
    
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"AWS API Error ({error_code}): {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    verify_table_data()