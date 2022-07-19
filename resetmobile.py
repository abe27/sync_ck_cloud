import asyncio
from datetime import datetime
import json
import shutil
import sys
import os
import time
import aiohttp
import psycopg2 as pgsql
import cx_Oracle
from nanoid import generate
from dotenv import load_dotenv
import requests
load_dotenv()

SPL_API_HOST = os.environ.get('SPL_SITE_URL')
SPL_API_USERNAME = os.environ.get('SPL_USERNAME')
SPL_API_PASSWORD = os.environ.get('SPL_PASSWORD')

DB_HOSTNAME = os.environ.get('DATABASE_URL')
DB_PORT = os.environ.get('DATABASE_PORT')
DB_NAME = os.environ.get('DATABASE_NAME')
DB_USERNAME = os.environ.get('DATABASE_USERNAME')
DB_PASSWORD = os.environ.get('DATABASE_PASSWORD')

ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')

# Initail Data

pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME,
                             dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Conn = pool.acquire()
# Oracon = cx_Oracle.connect(user=ORA_PASSWORD, password=ORA_USERNAME,dsn=ORA_DNS)
Ora = Conn.cursor()

def updatereqrunning():
    Ora.execute("UPDATE TXM_RUNINVNO r SET r.LASTINVNO = 0,r.UPDDTE=SYSDATE WHERE r.FACTORY = 'ALL'")
    

def resetmobile():
    Ora.execute("call SP_DOCUMENT()")

if __name__ == "__main__":
    updatereqrunning()
    resetmobile()
    Conn.commit()
    sys.exit(0)
    