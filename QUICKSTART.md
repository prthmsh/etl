# Quick Start Guide

## 🚀 Get Started in 5 Minutes

### Step 1: Install Dependencies

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install packages
pip install -r requirements.txt
```

### Step 2: Configure Environment

```bash
# Copy template and edit credentials
cp .env.template .env
nano .env  # Add your actual credentials
```

### Step 3: Setup MySQL Database

```bash
# Login to MySQL
mysql -u root -p

# Run the schema script
source config/schema.sql
```

Or manually:

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

### Step 4: Configure Your Servers

Edit `config/servers.yaml` with your FTP/SFTP server details:

```yaml
mysql:
  host: localhost
  port: 3306
  database: etl_db
  user: ${MYSQL_USER}
  password: ${MYSQL_PASSWORD}

ftp_servers:
  - name: my_server
    type: ftp
    host: ftp.myserver.com
    port: 21
    user: ${FTP_USER1}
    password: ${FTP_PASS1}
    file_path: /data/myfile.csv
    csv_params:
      delimiter: ','
      encoding: utf-8
      header: 0
```

### Step 5: Run the Pipeline

```bash
python main.py
```

## ✅ Verify It Works

Check the logs:
```bash
cat logs/etl.log
```

Query your data:
```sql
SELECT * FROM etl_db.etl_data LIMIT 10;
```

## 🎯 Next Steps

1. **Customize normalization** – Edit `main.py` to add your column mappings
2. **Add more servers** – Add entries to `config/servers.yaml`
3. **Schedule execution** – Use cron, Task Scheduler, or Airflow
4. **Monitor logs** – Set up log monitoring and alerts

## 🆘 Common Issues

**Can't connect to MySQL?**
```bash
# Test connection
mysql -h localhost -u etl_user -p etl_db
```

**FTP connection fails?**
```bash
# Test FTP from command line
ftp ftp.example.com
# Enter username and password
```

**Missing Python packages?**
```bash
# Reinstall all dependencies
pip install -r requirements.txt --force-reinstall
```

## 📚 Learn More

- Read the full [README.md](README.md)
- Check out [tests/test_normalizer.py](tests/test_normalizer.py) for examples
- Review [config/schema.sql](config/schema.sql) for database structure
