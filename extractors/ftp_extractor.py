import io
import ftplib
import paramiko
import pandas as pd
from pathlib import Path
from typing import Dict, Any
from utils.logger import setup_logger

logger = setup_logger(__name__)

class FTPExtractor:
    def __init__(self, server_config: Dict[str, Any]):
        self.config = server_config
        self.name = server_config['name']
        self.type = server_config['type']
        self.host = server_config['host']
        self.port = server_config.get('port', 21 if self.type == 'ftp' else 22)
        self.user = server_config['user']
        self.password = server_config.get('password')
        self.key_filename = server_config.get('key_filename')
        self.file_path = server_config['file_path']
        self.csv_params = server_config.get('csv_params', {})

    def _connect_ftp(self):
        """Connect to FTP server."""
        ftp = ftplib.FTP()
        ftp.connect(self.host, self.port)
        ftp.login(self.user, self.password)
        return ftp

    def _connect_sftp(self):
        """Connect to SFTP server using paramiko."""
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs = {
            'hostname': self.host,
            'port': self.port,
            'username': self.user,
        }
        if self.password:
            connect_kwargs['password'] = self.password
        if self.key_filename:
            connect_kwargs['key_filename'] = str(Path(self.key_filename).expanduser())
        ssh.connect(**connect_kwargs)
        sftp = ssh.open_sftp()
        return ssh, sftp

    def extract(self) -> pd.DataFrame:
        """Download CSV file and return as DataFrame."""
        logger.info(f"Extracting from {self.name} ({self.type}) - {self.file_path}")

        if self.type == 'ftp':
            return self._extract_ftp()
        elif self.type == 'sftp':
            return self._extract_sftp()
        else:
            raise ValueError(f"Unsupported server type: {self.type}")

    def _extract_ftp(self) -> pd.DataFrame:
        ftp = None
        try:
            ftp = self._connect_ftp()
            csv_data = io.BytesIO()
            ftp.retrbinary(f"RETR {self.file_path}", csv_data.write)
            csv_data.seek(0)
            df = pd.read_csv(csv_data, **self.csv_params)
            logger.info(f"Extracted {len(df)} rows from {self.name}")
            return df
        except Exception as e:
            logger.error(f"FTP extraction failed for {self.name}: {e}")
            raise
        finally:
            if ftp:
                ftp.quit()

    def _extract_sftp(self) -> pd.DataFrame:
        ssh = None
        sftp = None
        try:
            ssh, sftp = self._connect_sftp()
            with sftp.file(self.file_path, 'rb') as remote_file:
                csv_data = io.BytesIO(remote_file.read())
            csv_data.seek(0)
            df = pd.read_csv(csv_data, **self.csv_params)
            logger.info(f"Extracted {len(df)} rows from {self.name}")
            return df
        except Exception as e:
            logger.error(f"SFTP extraction failed for {self.name}: {e}")
            raise
        finally:
            if sftp:
                sftp.close()
            if ssh:
                ssh.close()
