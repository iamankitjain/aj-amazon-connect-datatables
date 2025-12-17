"""
Configuration loading utilities for Amazon Connect DataTables pipeline.
"""

import json
import os


def _load_json_config(config_path):
    """Helper function to load JSON configuration files."""
    # Validate path to prevent traversal attacks
    safe_path = os.path.normpath(config_path)
    if '..' in safe_path or os.path.isabs(safe_path):
        raise ValueError(f"Invalid config file path: {config_path}")
    
    try:
        if os.path.exists(safe_path):
            with open(safe_path, 'r') as f:
                return json.load(f)
        return None
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file {safe_path}: {str(e)}")
    except IOError as e:
        raise ValueError(f"Cannot read config file {safe_path}: {str(e)}")
    except Exception as e:
        raise ValueError(f"Error loading config file {safe_path}: {str(e)}")


def load_values_config(table_name):
    """
    Load table values from attribute_values/{table_name}.json configuration file.
    
    Args:
        table_name: Name of the table to load values for
        
    Returns:
        dict: Parsed JSON configuration with table values, None if not found
    """
    # Validate table name to prevent path traversal
    if '..' in table_name or '/' in table_name or '\\' in table_name:
        raise ValueError(f"Invalid table name: {table_name}")
    return _load_json_config(f'config/attribute_values/{table_name}.json')


def load_attributes_config(table_name):
    """
    Load attribute definitions from attributes/{table_name}.json configuration file.
    
    Args:
        table_name: Name of the table to load attribute definitions for
        
    Returns:
        dict: Parsed JSON configuration with attribute definitions, None if not found
    """
    # Validate table name to prevent path traversal
    if '..' in table_name or '/' in table_name or '\\' in table_name:
        raise ValueError(f"Invalid table name: {table_name}")
    return _load_json_config(f'config/attributes/{table_name}.json')


def load_default_config():
    """
    Provide fallback configuration when no config is provided in Lambda event.
    
    Returns:
        dict: Default configuration with sample table definition
    """
    try:
        config = {
            "instanceARN": "arn:aws:connect:us-east-1:123456789012:instance/12345678-1234-1234-1234-123456789012",
            "dataTables": [
                {
                    "name": "CustomerTypes",
                    "description": "Customer type lookup table",
                    "timeZone": "US/Eastern",
                    "valueLockLevel": "NONE",
                    "tags": {"Environment": "Production"}
                }
            ]
        }
        
        # Basic validation
        if not config.get('instanceARN') or not config.get('dataTables'):
            raise ValueError("Invalid default configuration structure")
        
        return config
        
    except Exception as e:
        raise ValueError(f"Error creating default configuration: {str(e)}")