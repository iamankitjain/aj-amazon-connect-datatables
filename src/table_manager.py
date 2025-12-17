"""
Data table management utilities for Amazon Connect.
"""

from botocore.exceptions import ClientError


class DataTableError(Exception):
    """Custom exception for data table operations."""
    pass


def get_existing_table(connect_client, instance_arn, table_name):
    """
    Check if a data table already exists in the Amazon Connect instance.

    Args:
        connect_client: Boto3 Amazon Connect client
        instance_arn: Amazon Connect instance ARN
        table_name: Name of the table to search for

    Returns:
        dict: Table info with DataTableId if found, None otherwise
    """
    try:
        # List all data tables in the instance
        response = connect_client.list_data_tables(
            InstanceId=instance_arn,
            MaxResults=100
        )

        # Search for table by name
        for table in response.get('DataTableSummaryList', []):
            if table['Name'] == table_name:
                return {'DataTableId': table['Id']}

        # Table not found
        return None

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code in ['AccessDeniedException', 'InvalidParameterException']:
            raise DataTableError(
                f'Failed to list data tables ({error_code}): {str(e)}')
        return None
    except Exception as e:
        raise DataTableError(
            f'Unexpected error checking table existence: {str(e)}')


def create_data_table(connect_client, instance_arn, table_config):
    """
    Create a new data table in Amazon Connect.

    Args:
        connect_client: Boto3 Amazon Connect client
        instance_arn: Amazon Connect instance ARN
        table_config: Dictionary with table configuration

    Returns:
        dict: API response with table creation details
    """
    try:
        return connect_client.create_data_table(
            InstanceId=instance_arn,
            Name=table_config['name'],
            Description=table_config.get('description', ''),
            TimeZone=table_config.get('timeZone', 'US/Eastern'),
            ValueLockLevel=table_config.get('valueLockLevel', 'NONE'),
            Status='PUBLISHED',
            Tags=table_config.get('tags', {})
        )
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        raise DataTableError(
            f'Failed to create data table ({error_code}): {str(e)}')
    except Exception as e:
        raise DataTableError(f'Unexpected error creating data table: {str(e)}')
