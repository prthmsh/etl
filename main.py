import os
# import yaml
# import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from extractors.ftp_extractor import FTPExtractor
from transformers.data_normalizer import DataNormalizer
from loaders.mysql_loader import MySQLLoader
from utils.logger import setup_logger

# Load environment variables from .env file
load_dotenv()

# Configure main logger
logger = setup_logger("etl_pipeline")

def resolve_env_vars(config: dict):
    """Recursively replace ${VAR} with values from environment."""
    if isinstance(config, dict):
        return {k: resolve_env_vars(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [resolve_env_vars(item) for item in config]
    elif isinstance(config, str) and config.startswith("${") and config.endswith("}"):
        env_var = config[2:-1]
        return os.getenv(env_var, config)
    else:
        return config

def load_config(config_path: str = "config/servers.yaml"):
    """Load and interpolate YAML config."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return resolve_env_vars(config)

def run_etl():
    logger.info("Starting ETL pipeline")

    # Load configuration
    config = load_config()
    mysql_config = config['mysql']
    ftp_servers = config['ftp_servers']

    # Test MySQL connection early
    loader = MySQLLoader(mysql_config, table_name="etl_data")
    loader.test_connection()

    # Initialize normalizer with custom rules
    normalizer = DataNormalizer({
        'required_columns': ['id', 'transaction_date'],
        'date_columns': ['transaction_date', 'created_at'],
        'column_mapping': {
            'transaction_id': 'id',
            'trans_date': 'transaction_date',
            'cust_name': 'customer_name'
        }
    })

    all_dfs = []
    for server_cfg in ftp_servers:
        try:
            # Extract
            extractor = FTPExtractor(server_cfg)
            df = extractor.extract()

            # Transform
            df_normalized = normalizer.normalize(df)

            # Collect or load immediately
            all_dfs.append(df_normalized)

            # Optionally load per server
            # loader.load(df_normalized, if_exists='append')
        except Exception as e:
            logger.error(f"Skipping server {server_cfg['name']} due to error: {e}")
            continue

    if all_dfs:
        # Combine all dataframes
        combined_df = pd.concat(all_dfs, ignore_index=True)
        # Final deduplication across sources
        combined_df = combined_df.drop_duplicates(subset=['id'] if 'id' in combined_df.columns else None)
        # Load into MySQL
        loader.load(combined_df, if_exists='replace')  # or 'append' depending on use case
        logger.info("ETL pipeline completed successfully")
    else:
        logger.warning("No data extracted from any server")

if __name__ == "__main__":
    run_etl()
