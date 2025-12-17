#!/usr/bin/env python3
"""
Local deployment script for Amazon Connect DataTables.

This script deploys data tables, attributes, and values to Amazon Connect
using the configuration files in the config/ directory.
"""

import json
import os
import sys

# Add parent directory to path to find config files
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
os.chdir(parent_dir)

from connect_datatables_handler import deploy_datatables

def load_config(config_file='config/data_tables_config.json'):
    """Load configuration from JSON file"""
    # Validate path to prevent traversal attacks
    safe_path = os.path.normpath(config_file)
    if '..' in safe_path or os.path.isabs(safe_path):
        raise ValueError("Invalid config file path")
    
    try:
        with open(safe_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Configuration file not found: {safe_path}")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON in configuration file: {safe_path}", e.doc, e.pos)
    except IOError as e:
        raise IOError(f"Cannot read configuration file: {safe_path} - {str(e)}")

def deploy_data_tables():
    """Deploy data tables using the pipeline"""
    
    try:
        # Load configuration
        config = load_config()
        
        # Execute deployment function
        result = deploy_datatables(config)
        
        # Print results
        print("Deployment Results:")
        print("=" * 50)
        
        for table_result in result['results']:
            status_icons = {'created': '[OK]', 'skipped': '[SKIP]'}
            status_icon = status_icons.get(table_result['status'], '[FAIL]')
            print(f"{status_icon} {table_result['name']}: {table_result['status']}")
            if 'message' in table_result:
                print(f"  - {table_result['message']}")
            if 'error' in table_result:
                print(f"  - Error: {table_result['error']}")
                
    except FileNotFoundError as e:
        print(f"[FAIL] Configuration file not found: {str(e)}")
    except json.JSONDecodeError as e:
        print(f"[FAIL] Invalid JSON in configuration file: {str(e)}")
    except ValueError as e:
        print(f"[FAIL] Configuration error: {str(e)}")
    except Exception as e:
        print(f"[FAIL] Pipeline failed: {str(e)}")

if __name__ == "__main__":
    deploy_data_tables()