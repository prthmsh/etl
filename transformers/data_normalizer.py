import pandas as pd
from utils.logger import setup_logger

logger = setup_logger(__name__)

class DataNormalizer:
    def __init__(self, rules: dict = None):
        """
        rules: optional dictionary with transformation parameters
        Example:
        {
            'required_columns': ['id', 'transaction_date'],
            'date_columns': ['transaction_date', 'created_at'],
            'column_mapping': {'trans_id': 'id', 'trans_date': 'transaction_date'},
            'numeric_columns': ['amount', 'quantity'],
            'default_values': {'status': 'pending'}
        }
        """
        self.rules = rules or {}

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply a series of cleaning and normalization steps."""
        logger.info(f"Starting normalization on {len(df)} rows")

        # 1. Standardize column names: lowercase, strip, replace spaces
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

        # 2. Remove duplicate rows
        initial_count = len(df)
        df = df.drop_duplicates()
        logger.info(f"Removed {initial_count - len(df)} duplicate rows")

        # 3. Handle missing values (customize as needed)
        # Example: drop rows where all required columns are NaN
        required_cols = self.rules.get('required_columns', [])
        if required_cols:
            df = df.dropna(subset=required_cols, how='all')

        # 4. Data type conversions
        # Convert date columns to datetime
        date_columns = self.rules.get('date_columns', [])
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Convert numeric columns
        numeric_columns = self.rules.get('numeric_columns', [])
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 5. Strip whitespace from object/string columns
        str_columns = df.select_dtypes(include=['object']).columns
        df[str_columns] = df[str_columns].apply(lambda x: x.str.strip() if hasattr(x, 'str') else x)

        # 6. Rename columns to match target DB schema
        column_mapping = self.rules.get('column_mapping', {})
        df = df.rename(columns=column_mapping)

        # 7. Apply default values for missing data
        default_values = self.rules.get('default_values', {})
        for col, default_val in default_values.items():
            if col in df.columns:
                df[col] = df[col].fillna(default_val)

        # 8. Add audit columns
        df['etl_loaded_at'] = pd.Timestamp.now()

        logger.info(f"Normalization complete, {len(df)} rows remain")
        return df
