"""
Value management utilities for Amazon Connect DataTables.

Implements update-first, then create logic for true upsert functionality.
Handles concurrency conflicts with automatic retry using fresh lock versions.
"""

import json
import logging
from botocore.exceptions import ClientError
from .config_loader import load_values_config, load_attributes_config
from .attribute_manager import get_table_lock_versions

logger = logging.getLogger(__name__)


def _format_list_value(attr_value, attr_type):
    """Format list values for TEXT_LIST and NUMBER_LIST attributes."""
    try:
        if attr_type == 'TEXT_LIST' and isinstance(attr_value, str):
            return json.dumps(attr_value.split(','))
        elif attr_type == 'NUMBER_LIST' and isinstance(attr_value, str):
            return json.dumps([float(x.strip()) for x in attr_value.split(',')])
        return attr_value
    except ValueError as e:
        raise ValueError(f"Cannot convert value '{attr_value}' for type {attr_type}: {str(e)}")
    except Exception as e:
        raise ValueError(f"Error formatting list value '{attr_value}': {str(e)}")


def _format_values_for_api(values_config, attributes_config, lock_versions, table_name):
    """Transform configuration data into API format."""
    attr_types = {attr['name']: attr['valueType'] for attr in attributes_config['attributes']}
    formatted_values = []
    
    for value_entry in values_config['values']:
        # Cache primary values to avoid recreating for each attribute
        primary_values = [{'AttributeName': pv['attributeName'], 'Value': pv['value']} 
                         for pv in value_entry['primaryValues']]
        
        # Pre-allocate list for better performance
        entry_attrs = value_entry['attributes']
        for attr in entry_attrs:
            attr_name = attr['attributeName']
            attr_type = attr_types.get(attr_name)
            
            # Only format if it's a list type to avoid unnecessary processing
            if attr_type in ('TEXT_LIST', 'NUMBER_LIST'):
                attr_value = _format_list_value(attr['value'], attr_type)
            else:
                attr_value = attr['value']
            
            formatted_values.append({
                'PrimaryValues': primary_values,
                'AttributeName': attr_name,
                'Value': str(attr_value),
                'LockVersion': lock_versions.get(attr_name, {})
            })
    
    return formatted_values


def _process_update_failures(failed_items, batch, values_to_create, retry_updates):
    """Process failed update items and categorize them for retry or creation."""
    failed_count = 0
    
    for failed_item in failed_items:
        original_value = next((v for v in batch 
                              if v['PrimaryValues'] == failed_item['PrimaryValues'] 
                              and v['AttributeName'] == failed_item['AttributeName']), None)
        
        if not original_value:
            continue
            
        message = failed_item.get('Message', '')
        if 'Value not found' in message:
            values_to_create.append(original_value)
        elif 'Concurrency conflict' in message:
            retry_updates.append(original_value)
        else:
            failed_count += 1
            logger.warning(f"Update failed (not retryable): {message}")
    
    return failed_count


def create_table_values(connect_client, instance_arn, data_table_id, table_name):
    """
    Populate data table with values using update-first, then create logic.
    
    First attempts to update all values, then creates any that failed with "Value not found".
    Handles DATA_TABLE lock level by refreshing lock versions between batches.
    Properly formats TEXT_LIST and NUMBER_LIST values as JSON arrays.
    
    Args:
        connect_client: Boto3 Amazon Connect client
        instance_arn: Amazon Connect instance ARN
        data_table_id: ID of the data table to populate
        table_name: Name of table (used to find config files)
        
    Returns:
        dict: Results summary with update/create counts and status
    """
    try:
        values_config = load_values_config(table_name)
        attributes_config = load_attributes_config(table_name)
        if not values_config or not attributes_config:
            return {'status': 'skipped', 'message': 'No values or attributes configuration found'}
        
        lock_versions = get_table_lock_versions(connect_client, instance_arn, data_table_id)
        if not lock_versions:
            return {'status': 'failed', 'error': 'Could not get lock versions for DATA_TABLE lock level'}
        
        formatted_values = _format_values_for_api(values_config, attributes_config, lock_versions, table_name)
        logger.info(f"Formatted {len(formatted_values)} attribute values for {table_name}")
        
        # Phase 1: Update existing values
        updated_count, failed_count, values_to_create = _process_updates(
            connect_client, instance_arn, data_table_id, table_name, formatted_values)
        
        # Phase 2: Create new values
        created_count, create_failed = _process_creates(
            connect_client, instance_arn, data_table_id, table_name, values_to_create)
        
        failed_count += create_failed
        total_processed = updated_count + created_count + failed_count
        
        logger.info(f"Value processing summary for {table_name}: {updated_count} updated, {created_count} created, {failed_count} failed, {total_processed} total")
        
        return {
            'status': 'completed',
            'updated': updated_count,
            'created': created_count,
            'failed': failed_count,
            'total': total_processed
        }
        
    except Exception as e:
        return {'status': 'failed', 'error': str(e)}


def _process_updates(connect_client, instance_arn, data_table_id, table_name, formatted_values):
    """Process update operations in batches."""
    batch_size = 25
    updated_count = 0
    failed_count = 0
    values_to_create = []
    
    logger.info(f"Phase 1: Attempting to update all {len(formatted_values)} values for {table_name}")
    
    for i in range(0, len(formatted_values), batch_size):
        if i > 0:
            _refresh_lock_versions(connect_client, instance_arn, data_table_id, 
                                 formatted_values, i, batch_size)
        
        batch = formatted_values[i:i + batch_size]
        batch_updated, batch_failed, batch_to_create = _process_single_update_batch(
            connect_client, instance_arn, data_table_id, table_name, batch)
        
        updated_count += batch_updated
        failed_count += batch_failed
        values_to_create.extend(batch_to_create)
    
    return updated_count, failed_count, values_to_create


def _process_creates(connect_client, instance_arn, data_table_id, table_name, values_to_create):
    """Process create operations in batches."""
    if not values_to_create:
        return 0, 0
    
    batch_size = 25
    created_count = 0
    failed_count = 0
    
    logger.info(f"Phase 2: Creating {len(values_to_create)} new values for {table_name}")
    
    for i in range(0, len(values_to_create), batch_size):
        _refresh_lock_versions(connect_client, instance_arn, data_table_id, 
                             values_to_create, i, batch_size)
        
        batch = values_to_create[i:i + batch_size]
        
        try:
            logger.debug(f"Creating batch of {len(batch)} new rows for {table_name}")
            response = connect_client.batch_create_data_table_value(
                InstanceId=instance_arn, DataTableId=data_table_id, Values=batch)
            
            successful = len(response.get('Successful', []))
            failed = len(response.get('Failed', []))
            created_count += successful
            failed_count += failed
            
            logger.debug(f"Batch create result: {successful} created, {failed} failed")
            
        except ClientError as e:
            failed_count += len(batch)
            logger.error(f"Batch create failed for {table_name}: {str(e)}")
    
    return created_count, failed_count


def _refresh_lock_versions(connect_client, instance_arn, data_table_id, values, start_idx, batch_size):
    """Refresh lock versions for a batch of values."""
    lock_versions = get_table_lock_versions(connect_client, instance_arn, data_table_id)
    if lock_versions:
        for j in range(start_idx, min(start_idx + batch_size, len(values))):
            attr_name = values[j]['AttributeName']
            values[j]['LockVersion'] = lock_versions.get(attr_name, {})


def _process_single_update_batch(connect_client, instance_arn, data_table_id, table_name, batch):
    """Process a single update batch with retry logic."""
    try:
        logger.debug(f"Updating batch of {len(batch)} rows for {table_name}")
        response = connect_client.batch_update_data_table_value(
            InstanceId=instance_arn, DataTableId=data_table_id, Values=batch)
        
        successful_updates = len(response.get('Successful', []))
        values_to_create = []
        retry_updates = []
        
        failed_count = _process_update_failures(
            response.get('Failed', []), batch, values_to_create, retry_updates)
        
        # Handle retries
        if retry_updates:
            retry_successful, retry_failed = _retry_concurrency_conflicts(
                connect_client, instance_arn, data_table_id, table_name, retry_updates)
            successful_updates += retry_successful
            failed_count += retry_failed
        
        logger.debug(f"Batch update result: {successful_updates} updated, {len(response.get('Failed', []))} failed")
        return successful_updates, failed_count, values_to_create
        
    except ClientError as e:
        logger.error(f"Batch update failed for {table_name}: {str(e)}")
        return 0, len(batch), []


def _retry_concurrency_conflicts(connect_client, instance_arn, data_table_id, table_name, retry_updates):
    """Retry concurrency conflicts with fresh lock versions."""
    logger.info(f"Retrying {len(retry_updates)} concurrency conflicts for {table_name}")
    fresh_lock_versions = get_table_lock_versions(connect_client, instance_arn, data_table_id)
    
    if not fresh_lock_versions:
        return 0, len(retry_updates)
    
    for retry_value in retry_updates:
        attr_name = retry_value['AttributeName']
        retry_value['LockVersion'] = fresh_lock_versions.get(attr_name, {})
    
    try:
        retry_response = connect_client.batch_update_data_table_value(
            InstanceId=instance_arn, DataTableId=data_table_id, Values=retry_updates)
        
        retry_successful = len(retry_response.get('Successful', []))
        retry_failed = len(retry_response.get('Failed', []))
        
        logger.debug(f"Retry result: {retry_successful} updated, {retry_failed} failed")
        return retry_successful, retry_failed
        
    except ClientError as e:
        logger.error(f"Retry failed for {table_name}: {str(e)}")
        return 0, len(retry_updates)