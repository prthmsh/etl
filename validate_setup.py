#!/usr/bin/env python3
"""
Setup Validation Script
Checks if the ETL project is properly configured and ready to run.
"""

import os
import sys
from pathlib import Path

def check_file_exists(filepath, description):
    """Check if a file exists."""
    if Path(filepath).exists():
        print(f"✅ {description}: {filepath}")
        return True
    else:
        print(f"❌ {description} NOT FOUND: {filepath}")
        return False

def check_env_variable(var_name):
    """Check if environment variable is set."""
    if os.getenv(var_name):
        print(f"✅ Environment variable set: {var_name}")
        return True
    else:
        print(f"❌ Environment variable NOT SET: {var_name}")
        return False

def check_python_version():
    """Check Python version."""
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print(f"✅ Python version: {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"❌ Python version {version.major}.{version.minor} is too old. Need 3.8+")
        return False

def check_packages():
    """Check if required packages are installed."""
    required_packages = [
        'pandas',
        'sqlalchemy',
        'pymysql',
        'paramiko',
        'yaml',
        'dotenv'
    ]
    
    all_installed = True
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ Package installed: {package}")
        except ImportError:
            print(f"❌ Package NOT INSTALLED: {package}")
            all_installed = False
    
    return all_installed

def check_mysql_connection():
    """Try to connect to MySQL."""
    try:
        from dotenv import load_dotenv
        import yaml
        from loaders.mysql_loader import MySQLLoader
        
        load_dotenv()
        
        # Load config
        with open('config/servers.yaml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Resolve env vars in config
        mysql_config = {
            'host': config['mysql']['host'],
            'port': config['mysql']['port'],
            'database': config['mysql']['database'],
            'user': os.getenv('MYSQL_USER'),
            'password': os.getenv('MYSQL_PASSWORD')
        }
        
        loader = MySQLLoader(mysql_config, 'etl_data')
        loader.test_connection()
        print("✅ MySQL connection successful")
        return True
    except Exception as e:
        print(f"❌ MySQL connection failed: {e}")
        return False

def main():
    """Run all validation checks."""
    print("=" * 60)
    print("ETL Project Setup Validation")
    print("=" * 60)
    print()
    
    checks = []
    
    # Check Python version
    print("Checking Python version...")
    checks.append(check_python_version())
    print()
    
    # Check required files
    print("Checking required files...")
    checks.append(check_file_exists('.env', '.env file'))
    checks.append(check_file_exists('config/servers.yaml', 'Server config'))
    checks.append(check_file_exists('requirements.txt', 'Requirements file'))
    checks.append(check_file_exists('main.py', 'Main script'))
    print()
    
    # Check environment variables
    print("Checking environment variables...")
    from dotenv import load_dotenv
    load_dotenv()
    
    checks.append(check_env_variable('MYSQL_USER'))
    checks.append(check_env_variable('MYSQL_PASSWORD'))
    print()
    
    # Check packages
    print("Checking Python packages...")
    checks.append(check_packages())
    print()
    
    # Check MySQL connection
    print("Checking MySQL connection...")
    checks.append(check_mysql_connection())
    print()
    
    # Summary
    print("=" * 60)
    passed = sum(checks)
    total = len(checks)
    
    if passed == total:
        print(f"🎉 All checks passed! ({passed}/{total})")
        print("You're ready to run: python main.py")
        return 0
    else:
        print(f"⚠️  Some checks failed ({passed}/{total} passed)")
        print("Please fix the issues above before running the pipeline.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
