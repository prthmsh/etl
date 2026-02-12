# Multi-FTP ETL Pipeline

A production-ready ETL (Extract, Transform, Load) pipeline that extracts CSV files from multiple FTP/SFTP servers, normalizes the data using pandas, and loads it into a MySQL database.

## 🚀 Features

- **Multi-source extraction** – Connect to multiple FTP/SFTP servers simultaneously
- **Flexible data normalization** – Column standardization, duplicate removal, date parsing, type conversions
- **Efficient MySQL loading** – Bulk insert with SQLAlchemy, connection pooling, and chunking
- **Comprehensive logging** – Console and rotating file logs with detailed tracking
- **Configuration-driven** – YAML configuration with environment variable interpolation
- **Secure credential management** – Environment variables via `.env` file
- **Error handling** – Graceful error handling with server-level fault isolation
- **Modular architecture** – Easy to extend and customize

## 📁 Project Structure

```
etl_project/
├── config/
│   └── servers.yaml              # FTP/SFTP server configurations
├── extractors/
│   ├── __init__.py
│   └── ftp_extractor.py          # FTP/SFTP connection and CSV extraction
├── transformers/
│   ├── __init__.py
│   └── data_normalizer.py        # Pandas-based data normalization
├── loaders/
│   ├── __init__.py
│   └── mysql_loader.py           # MySQL bulk insert using SQLAlchemy
├── utils/
│   ├── __init__.py
│   └── logger.py                 # Logging configuration with rotation
├── logs/                         # Log files (auto-created)
├── data/                         # Sample data files (optional)
├── tests/                        # Unit tests (future)
├── main.py                       # ETL orchestration script
├── requirements.txt              # Python dependencies
├── .env                          # Environment variables (DO NOT commit)
├── .gitignore                    # Git ignore rules
└── README.md                     # This file
```

## 🛠️ Prerequisites

- **Python 3.8+**
- **MySQL 5.7+** or **MariaDB 10.3+**
- **FTP/SFTP server access** with credentials
- **pip** package manager

## 📦 Installation

### 1. Clone the repository

```bash
git clone <your-repository-url>
cd etl_project
```

### 2. Create and activate virtual environment

**Linux/Mac:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

**Dependencies:**
- `pandas==2.0.3` – Data manipulation
- `sqlalchemy==2.0.19` – Database ORM
- `pymysql==1.1.0` – MySQL driver
- `paramiko==3.3.1` – SFTP support
- `pyyaml==6.0` – YAML configuration
- `python-dotenv==1.0.0` – Environment variables
- `cryptography==41.0.3` – SSH encryption

### 4. Configure environment variables

Copy the template and edit with your credentials:

```bash
cp .env.template .env
nano .env  # or use your preferred editor
```

**Example `.env` file:**
```ini
MYSQL_USER=etl_user
MYSQL_PASSWORD=your_secure_password
FTP_USER1=ftp_reader
FTP_PASS1=ftp_secret
SFTP_USER=sftp_reader
SFTP_PASS=sftp_secret
```

### 5. Configure servers

Edit `config/servers.yaml` to define your FTP/SFTP servers and MySQL connection:

```yaml
mysql:
  host: localhost
  port: 3306
  database: etl_db
  user: ${MYSQL_USER}
  password: ${MYSQL_PASSWORD}

ftp_servers:
  - name: sales_server
    type: ftp
    host: ftp.example.com
    port: 21
    user: ${FTP_USER1}
    password: ${FTP_PASS1}
    file_path: /data/sales_2023.csv
    csv_params:
      delimiter: ','
      encoding: utf-8
      header: 0
```

### 6. Prepare MySQL database

Create the database and target table:

```sql
CREATE DATABASE etl_db;
USE etl_db;

CREATE TABLE etl_data (
    id VARCHAR(50) PRIMARY KEY,
    transaction_date DATETIME,
    customer_name VARCHAR(255),
    amount DECIMAL(10,2),
    status VARCHAR(50),
    created_at DATETIME,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🚀 Usage

### Run the ETL pipeline

```bash
python main.py
```

### What happens:

1. **Configuration Loading** – Reads `servers.yaml` and resolves environment variables
2. **Connection Testing** – Tests MySQL connectivity before processing
3. **Extraction** – Downloads CSV files from each configured FTP/SFTP server
4. **Transformation** – Normalizes data (standardizes columns, removes duplicates, parses dates)
5. **Loading** – Combines all data and bulk inserts into MySQL
6. **Logging** – Writes detailed logs to console and `logs/etl.log`

### Output example:

```
2025-02-12 10:30:15 - etl_pipeline - INFO - Starting ETL pipeline
2025-02-12 10:30:15 - etl_pipeline - INFO - MySQL connection successful
2025-02-12 10:30:16 - extractors.ftp_extractor - INFO - Extracting from sales_server (ftp) - /data/sales_2023.csv
2025-02-12 10:30:18 - extractors.ftp_extractor - INFO - Extracted 1250 rows from sales_server
2025-02-12 10:30:18 - transformers.data_normalizer - INFO - Starting normalization on 1250 rows
2025-02-12 10:30:18 - transformers.data_normalizer - INFO - Removed 12 duplicate rows
2025-02-12 10:30:18 - transformers.data_normalizer - INFO - Normalization complete, 1238 rows remain
2025-02-12 10:30:20 - loaders.mysql_loader - INFO - Loading 2456 rows into etl_data
2025-02-12 10:30:22 - loaders.mysql_loader - INFO - Successfully loaded 2456 rows
2025-02-12 10:30:22 - etl_pipeline - INFO - ETL pipeline completed successfully
```

## ⚙️ Customization

### Modify normalization rules

Edit `main.py` to customize the `DataNormalizer`:

```python
normalizer = DataNormalizer({
    'required_columns': ['id', 'transaction_date'],
    'date_columns': ['transaction_date', 'created_at'],
    'numeric_columns': ['amount', 'quantity'],
    'column_mapping': {
        'transaction_id': 'id',
        'trans_date': 'transaction_date',
        'cust_name': 'customer_name'
    },
    'default_values': {
        'status': 'pending'
    }
})
```

### Add more FTP servers

Simply add new entries to `config/servers.yaml`:

```yaml
ftp_servers:
  - name: new_server
    type: sftp
    host: sftp.newserver.com
    port: 22
    user: ${NEW_USER}
    password: ${NEW_PASS}
    file_path: /data/new_data.csv
    csv_params:
      delimiter: ','
      encoding: utf-8
```

### Change load strategy

Modify the load behavior in `main.py`:

```python
# Replace entire table
loader.load(combined_df, if_exists='replace')

# Append to existing data
loader.load(combined_df, if_exists='append')

# Load per server instead of combined
for server_cfg in ftp_servers:
    # ... extract and transform ...
    loader.load(df_normalized, if_exists='append')
```

## 🧪 Testing

### Test individual components

```bash
# Test MySQL connection
python -c "from loaders.mysql_loader import MySQLLoader; from dotenv import load_dotenv; import yaml; load_dotenv(); config = yaml.safe_load(open('config/servers.yaml')); loader = MySQLLoader(config['mysql'], 'etl_data'); loader.test_connection()"

# Test FTP extraction (modify with your server details)
python -c "from extractors.ftp_extractor import FTPExtractor; import yaml; config = yaml.safe_load(open('config/servers.yaml'))['ftp_servers'][0]; extractor = FTPExtractor(config); df = extractor.extract(); print(df.head())"
```

## 📊 Scheduling

### Using cron (Linux/Mac)

```bash
# Edit crontab
crontab -e

# Run daily at 2:00 AM
0 2 * * * cd /path/to/etl_project && /path/to/venv/bin/python main.py >> logs/cron.log 2>&1
```

### Using Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (daily, weekly, etc.)
4. Action: Start a program
5. Program: `C:\path\to\venv\Scripts\python.exe`
6. Arguments: `C:\path\to\etl_project\main.py`
7. Start in: `C:\path\to\etl_project`

### Using Apache Airflow

Create a DAG file in your Airflow dags folder:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Multi-FTP ETL Pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
)

run_etl = BashOperator(
    task_id='run_etl',
    bash_command='cd /path/to/etl_project && /path/to/venv/bin/python main.py',
    dag=dag,
)
```

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| **MySQL connection fails** | Verify host, port, credentials, and database exists. Check firewall rules. |
| **FTP connection timeout** | Ensure server is reachable. Check firewall/NAT settings. Try passive mode. |
| **SFTP authentication fails** | Verify SSH key permissions (600). Check username and key_filename path. |
| **CSV parsing errors** | Adjust `csv_params` (delimiter, encoding, header) in `servers.yaml`. |
| **Column not found errors** | Verify column names in CSV match `column_mapping` rules. |
| **Permission denied on logs/** | Ensure write permissions: `chmod 755 logs/` |
| **Module not found** | Activate virtual environment: `source venv/bin/activate` |

## 🔒 Security Best Practices

- ✅ Never commit `.env` file to version control
- ✅ Use SSH keys instead of passwords for SFTP when possible
- ✅ Set restrictive permissions on `.env`: `chmod 600 .env`
- ✅ Use database user with minimal required privileges
- ✅ Enable SSL/TLS for MySQL connections in production
- ✅ Rotate credentials regularly
- ✅ Use encrypted connections (SFTP over FTP)

## 📈 Performance Optimization

- **Chunking**: Adjust `chunksize` in `MySQLLoader.load()` for large datasets
- **Parallel extraction**: Modify `main.py` to use `concurrent.futures` for parallel downloads
- **Indexing**: Add indexes on frequently queried columns in MySQL
- **Connection pooling**: Already enabled via SQLAlchemy
- **Incremental loads**: Implement delta detection based on timestamps or checksums

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit changes: `git commit -am 'Add feature'`
4. Push to branch: `git push origin feature-name`
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🙋 Support

For issues or questions:
- Create an issue in the repository
- Check existing documentation
- Review logs in `logs/etl.log`

---

**Built with ❤️ for efficient data integration**
