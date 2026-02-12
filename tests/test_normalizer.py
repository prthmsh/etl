import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from transformers.data_normalizer import DataNormalizer

class TestDataNormalizer(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.sample_data = pd.DataFrame({
            'Transaction_ID': ['T001', 'T002', 'T003', 'T001'],
            'Trans_Date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-01'],
            'Cust_Name': [' John Doe ', ' Jane Smith ', ' Bob Johnson ', ' John Doe '],
            'Amount': ['100.50', '200.75', '150.25', '100.50']
        })
        
    def test_column_standardization(self):
        """Test that column names are standardized."""
        normalizer = DataNormalizer()
        result = normalizer.normalize(self.sample_data.copy())
        
        # Check that columns are lowercase with underscores
        expected_columns = ['transaction_id', 'trans_date', 'cust_name', 'amount']
        for col in expected_columns:
            self.assertIn(col, result.columns)
    
    def test_duplicate_removal(self):
        """Test that duplicate rows are removed."""
        normalizer = DataNormalizer()
        result = normalizer.normalize(self.sample_data.copy())
        
        # Original has 4 rows, should be 3 after deduplication
        self.assertEqual(len(result), 3)
    
    def test_whitespace_stripping(self):
        """Test that whitespace is stripped from string columns."""
        normalizer = DataNormalizer()
        result = normalizer.normalize(self.sample_data.copy())
        
        # Check that names don't have leading/trailing spaces
        self.assertEqual(result['cust_name'].iloc[0], 'John Doe')
        self.assertEqual(result['cust_name'].iloc[1], 'Jane Smith')
    
    def test_column_mapping(self):
        """Test that column mapping works correctly."""
        normalizer = DataNormalizer({
            'column_mapping': {
                'transaction_id': 'id',
                'trans_date': 'transaction_date',
                'cust_name': 'customer_name'
            }
        })
        result = normalizer.normalize(self.sample_data.copy())
        
        # Check that columns are renamed
        self.assertIn('id', result.columns)
        self.assertIn('transaction_date', result.columns)
        self.assertIn('customer_name', result.columns)
    
    def test_date_parsing(self):
        """Test that date columns are parsed correctly."""
        normalizer = DataNormalizer({
            'date_columns': ['trans_date']
        })
        result = normalizer.normalize(self.sample_data.copy())
        
        # Check that trans_date is datetime type
        self.assertEqual(result['trans_date'].dtype, 'datetime64[ns]')
    
    def test_audit_column_added(self):
        """Test that audit column is added."""
        normalizer = DataNormalizer()
        result = normalizer.normalize(self.sample_data.copy())
        
        # Check that etl_loaded_at column exists
        self.assertIn('etl_loaded_at', result.columns)

if __name__ == '__main__':
    unittest.main()
