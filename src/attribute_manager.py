"""
Attribute management utilities for Amazon Connect DataTables.
"""

import logging
from botocore.exceptions import ClientError
from .config_loader import load_attributes_config

logger = logging.getLogger(__name__)


def create_table_attributes(connect_client, instance_arn, data_table_id, table_name):
    """
    Create attributes for a data table based on configuration files.
    
    Loads attribute definitions from attributes/{table_name}.json and creates
    each attribute with proper validation rules and data types.
    
    Args:
        connect_client: Boto3 Amazon Connect client
        instance_arn: Amazon Connect instance ARN
        data_table_id: ID of the data table to add attributes to
        table_name: Name of table (used to find config file)
        
    Returns:
        dict: Results summary with status and individual attribute results
    """
    try:
        # Load attribute definitions from JSON configuration file
        attributes_config = load_attributes_config(table_name)
        logger.info(f"Loaded attributes config for {table_name}: {attributes_config is not None}")
        if not attributes_config:
            return {'status': 'skipped', 'message': 'No attributes configuration found'}
        
        logger.info(f"Processing {len(attributes_config['attributes'])} attributes for {table_name}")
        attribute_results = []
        
        # Process each attribute definition
        for attr_config in attributes_config['attributes']:
            attr_name = attr_config['name']
            
            # Skip if attribute already exists to avoid duplicates
            if attribute_exists(connect_client, instance_arn, data_table_id, attr_name):
                attribute_results.append({
                    'name': attr_name,
                    'status': 'skipped',
                    'message': 'Attribute already exists'
                })
                continue
            
            # Create new attribute with configuration
            try:
                # Build base attribute parameters
                create_params = {
                    'InstanceId': instance_arn,
                    'DataTableId': data_table_id,
                    'Name': attr_name,
                    'ValueType': attr_config['valueType'],
                    'Description': attr_config.get('description', ''),
                    'Primary': attr_config.get('primary', False)
                }
                
                # Add validation rules if specified in configuration
                if 'validation' in attr_config:
                    create_params['Validation'] = format_validation(attr_config['validation'])
                
                # Create the attribute via API
                response = connect_client.create_data_table_attribute(**create_params)
                logger.debug(f"Create attribute response: {response}")
                
                # Record successful creation
                attribute_results.append({
                    'name': attr_name,
                    'status': 'created',
                    'attributeArn': response.get('AttributeArn', response.get('Arn', 'Unknown'))
                })
                
            except ClientError as e:
                # Record attribute creation failure
                attribute_results.append({
                    'name': attr_name,
                    'status': 'failed',
                    'error': str(e)
                })
        
        # Return overall results
        return {
            'status': 'completed',
            'attributes': attribute_results
        }
        
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Exception in create_table_attributes: {str(e)}")
        return {
            'status': 'failed',
            'error': str(e),
            'attributes': []
        }


def attribute_exists(connect_client, instance_arn, data_table_id, attr_name):
    """
    Check if a specific attribute already exists in the data table.
    
    Args:
        connect_client: Boto3 Amazon Connect client
        instance_arn: Amazon Connect instance ARN
        data_table_id: ID of the data table to check
        attr_name: Name of attribute to search for
        
    Returns:
        bool: True if attribute exists, False otherwise
    """
    try:
        # Get list of all attributes in the table
        response = connect_client.list_data_table_attributes(
            InstanceId=instance_arn,
            DataTableId=data_table_id,
            MaxResults=100
        )
        
        # Extract attribute names and check for match
        existing_attrs = [attr['Name'] for attr in response.get('Attributes', [])]
        return attr_name in existing_attrs
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code in ['AccessDeniedException', 'ResourceNotFoundException']:
            logger.error(f"Critical error checking attribute existence ({error_code}): {str(e)}")
            raise
        logger.warning(f"Non-critical error checking attribute existence: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking attribute existence: {str(e)}")
        return False


def format_validation(validation_config):
    """
    Convert validation configuration from JSON format to Amazon Connect API format.
    
    Transforms camelCase configuration keys to PascalCase API keys and
    handles special cases like enum validation.
    
    Args:
        validation_config: Dict with validation rules from JSON config
        
    Returns:
        dict: Formatted validation rules for Amazon Connect API
    """
    validation = {}
    
    # Map JSON configuration keys to Amazon Connect API keys
    key_mapping = {
        'minLength': 'MinLength',
        'maxLength': 'MaxLength', 
        'minValues': 'MinValues',
        'maxValues': 'MaxValues',
        'ignoreCase': 'IgnoreCase',
        'minimum': 'Minimum',
        'maximum': 'Maximum',
        'exclusiveMinimum': 'ExclusiveMinimum',
        'exclusiveMaximum': 'ExclusiveMaximum',
        'multipleOf': 'MultipleOf'
    }
    
    # Apply standard validation mappings
    for config_key, api_key in key_mapping.items():
        if config_key in validation_config:
            validation[api_key] = validation_config[config_key]
    
    # Handle enum validation with special structure
    if 'enum' in validation_config:
        validation['Enum'] = {
            'Strict': validation_config['enum'].get('strict', True),
            'Values': validation_config['enum'].get('values', [])
        }
    
    return validation


def get_table_lock_versions(connect_client, instance_arn, data_table_id):
    """
    Retrieve current lock versions for all attributes in a data table.
    
    Lock versions are required for DATA_TABLE lock level and change after
    each successful operation, so they must be refreshed between batches.
    
    Args:
        connect_client: Boto3 Amazon Connect client
        instance_arn: Amazon Connect instance ARN
        data_table_id: ID of the data table
        
    Returns:
        dict: Mapping of attribute names to their current lock versions,
              None if retrieval fails
    """
    try:
        # Get all attributes and their current lock versions
        response = connect_client.list_data_table_attributes(
            InstanceId=instance_arn,
            DataTableId=data_table_id,
            MaxResults=100
        )
        
        # Build mapping of attribute name to lock version
        lock_versions = {}
        for attr in response.get('Attributes', []):
            attr_name = attr['Name']
            lock_versions[attr_name] = attr.get('LockVersion', {})
        
        return lock_versions
        
    except ClientError as e:
        # Log error and return None to indicate failure
        logger.error(f"Error getting lock versions: {e}")
        return None