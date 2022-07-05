from datetime import datetime
import shutil
import sys
import os
import time
import psycopg2 as pgsql
import cx_Oracle
from nanoid import generate
from sqlalchemy import true
from spllibs import Yazaki, SplApi, SplSharePoint, LogActivity as log
from os.path import join, dirname
from dotenv import load_dotenv
# dotenv_path = join(dirname(__file__), '.env')
# load_dotenv(dotenv_path)
load_dotenv()

SERVICE_TYPE = "CK2"
YAZAKI_HOST = f"https://{os.environ.get('HOST')}:{os.environ.get('PORT')}"
YAZAKI_USER = os.environ.get('CK_USERNAME')
YAZAKI_PASSWORD = os.environ.get('CK_PASSWORD')

SPL_API_HOST = os.environ.get('SPL_SITE_URL')
SPL_API_USERNAME = os.environ.get('SPL_USERNAME')
SPL_API_PASSWORD = os.environ.get('SPL_PASSWORD')

SHAREPOINT_SITE_URL = os.environ.get('SHAREPOINT_URL')
SHAREPOINT_SITE_NAME = os.environ.get('SHAREPOINT_URL_SITE')
SHAREPOINT_USERNAME = os.environ.get('SHAREPOINT_USERNAME')
SHAREPOINT_PASSWORD = os.environ.get('SHAREPOINT_PASSWORD')

DB_HOSTNAME = os.environ.get('DATABASE_URL')
DB_PORT = os.environ.get('DATABASE_PORT')
DB_NAME = os.environ.get('DATABASE_NAME')
DB_USERNAME = os.environ.get('DATABASE_USERNAME')
DB_PASSWORD = os.environ.get('DATABASE_PASSWORD')

ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')

pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME,
                             dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Oracur = Oracon.cursor()

# Initail PostgreSQL Server
mydb = pgsql.connect(
    host=DB_HOSTNAME,
    port=DB_PORT,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    database="stkbackup",
)

db_cursor = mydb.cursor()


def main():
    sql = f"""SELECT 
        TO_CHAR(ts.SYSDTE, 'YYYY-MM-DD') stock_date,ts.PARTNO part_no,tc.LOTNO lotno,ts.RUNNINGNO serial_no,ts.STOCKQUANTITY qty,tc.SHELVE on_shelve,CASE WHEN tc.PALLETKEY IS NULL THEN '-' ELSE tc.PALLETKEY END palletno,ts.STKTAKECHKFLG checkflg
    FROM TXP_STKTAKECARTON ts
    LEFT JOIN TXP_CARTONDETAILS tc ON ts.PARTNO=tc.PARTNO AND ts.RUNNINGNO=tc.RUNNINGNO
    ORDER BY ts.PARTNO,tc.LOTNO,ts.RUNNINGNO,ts.SHELVE,tc.PALLETKEY"""
    db = Oracur.execute(sql)
    r = 1
    for i in db.fetchall():
        stock_date = str(i[0]).strip()
        part_no = str(i[1]).strip()
        lotno = str(i[2]).strip()
        serial_no = str(i[3]).strip()
        qty = str(i[4]).strip()
        on_shelve = str(i[5]).strip()
        palletno = str(i[6]).strip()
        checkflg = 'false'
        if str(i[7]).strip() == '1':checkflg = 'true'
        sql_check=f"select part_no from tbt_check_stocks where stock_date='{stock_date}' and part_no='{part_no}' and lotno='{lotno}' and serial_no='{serial_no}'"
        db_cursor.execute(sql_check)
        part = db_cursor.fetchone()
        
        txt = "insert"
        sql_part = f"""insert into tbt_check_stocks(stock_date, part_no, serial_no, qty, last_update, on_shelve, lotno, checkflg, palletno)values('{stock_date}', '{part_no}', '{serial_no}', '{qty}', current_timestamp, '{on_shelve}', '{lotno}', {checkflg}, '{palletno}')"""
        if part != None:
            txt = "update"
            sql_part = f"""update tbt_check_stocks set last_update=current_timstamp, on_shelve='{on_shelve}', palletno='{palletno}' where stock_date='{stock_date}' and part_no='{part_no}' and lotno='{lotno}' and serial_no='{serial_no}'"""

        db_cursor.execute(sql_part)
        print(f"{r}. serial no: {serial_no} is {txt}")
        r += 1

if __name__ == '__main__':
    main()
    pool.release(Oracon)
    pool.close()
    mydb.commit()
    mydb.close()
    sys.exit(0)
