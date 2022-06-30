import sys
import os
import pandas as pd
import cx_Oracle
import psycopg2 as pgsql
from datetime import datetime
from nanoid import generate
from dotenv import load_dotenv
load_dotenv()

DB_HOSTNAME = os.environ.get('DATABASE_URL')
DB_PORT = os.environ.get('DATABASE_PORT')
DB_NAME = 'stkdb'
DB_USERNAME = os.environ.get('DATABASE_USERNAME')
DB_PASSWORD = os.environ.get('DATABASE_PASSWORD')

ORA_DNS=f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME=os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD=os.environ.get('ORAC_DB_PASSWORD')

pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Oracur = Oracon.cursor()

### Initail PostgreSQL Server
pgdb = pgsql.connect(
    host=DB_HOSTNAME,
    port=DB_PORT,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    database=DB_NAME,
)
pg_cursor = pgdb.cursor()

def ins(shelve, i):
    part_no = str(i["part_no"])
    qty = int(i["qty"])
    ctn = int(i["ctn"])
    
    sql = f"select part_no,qty,ctn from tbt_stocks where part_no='{part_no}' and shelve='{shelve}'"
    pg_cursor.execute(sql)
    pg = pg_cursor.fetchone()
    txt = f"UPDATE {part_no}"
    _qty = qty
    _ctn = ctn
    if pg != None:
        _qty = (qty + int(pg[1]))
        _ctn = (ctn + int(pg[2]))
    
    sql_insert = f"update tbt_stocks set qty ={_qty},ctn={_ctn} where part_no='{part_no}' and shelve='{shelve}'"
    if pg is None:
        txt = f"INSERT {part_no}"
        sql_insert = f"""insert into tbt_stocks(whs, part_no, qty, ctn, created_at, shelve, spl_check)values('CK', '{part_no}', {_qty}, {_ctn}, current_timestamp, '{shelve}', 0)"""
    
    pg_cursor.execute(sql_insert)
    print(txt + f" QTY: {_qty} CTN: {_ctn} SHELVE: {shelve}")

def check_stock(sql):
    ctn = Oracur.execute(sql)
    return int(ctn.fetchone()[0])

def p58():
    # df = pd.read_excel("stocks/p58.xlsx", index_col=None)
    # data = df.to_dict('records')
    # for i in data:
    #     ins('P5', i)
        
    sql = f"select part_no,shelve,spl_check from tbt_stocks where shelve='P5'"
    pg_cursor.execute(sql)
    db = pg_cursor.fetchall()
    for i in db:
        check_ctn = check_stock(f"SELECT count(PARTNO) FROM TXP_STKTAKECARTON t WHERE t.PARTNO='{i[0]}' AND STKTAKECHKFLG IS NOT NULL AND SHELVE IN ('S-P58', 'S-P59')")
        pg_cursor.execute(f"update tbt_stocks set spl_check={check_ctn} where part_no='{i[0]}' and shelve='P5'")
        print(f"CHECK S-P5 PARTNO: {i[0]}")
        
def hold():
    # df = pd.read_excel("stocks/hold.xlsx", index_col=None)
    # data = df.to_dict('records')
    # for i in data:
    #     ins('S-HOLD', i)
        
    sql = f"select part_no,shelve,spl_check from tbt_stocks where shelve='S-HOLD'"
    pg_cursor.execute(sql)
    db = pg_cursor.fetchall()
    for i in db:
        check_ctn = check_stock(f"SELECT count(PARTNO) FROM TXP_STKTAKECARTON t WHERE t.PARTNO='{i[0]}' AND STKTAKECHKFLG IS NOT NULL AND SHELVE IN ('S-HOLD')")
        pg_cursor.execute(f"update tbt_stocks set spl_check={check_ctn} where part_no='{i[0]}' and shelve='S-HOLD'")
        print(f"CHECK S-HOLD PARTNO: {i[0]}")
        
def stock():
    # df = pd.read_excel("stocks/stock.xlsx", index_col=None)
    # data = df.to_dict('records')
    # for i in data:
    #     ins('SHELVE', i)
    
    sql = f"select part_no,shelve,spl_check from tbt_stocks where shelve='SHELVE'"
    pg_cursor.execute(sql)
    db = pg_cursor.fetchall()
    for i in db:
        check_ctn = check_stock(f"SELECT count(PARTNO) FROM TXP_STKTAKECARTON t WHERE t.PARTNO='{i[0]}' AND STKTAKECHKFLG IS NOT NULL AND SHELVE NOT IN ('S-PLOUT', 'S-HOLD', 'S-P58', 'S-P59', 'S-CK1', 'S-XXX')")
        pg_cursor.execute(f"update tbt_stocks set spl_check={check_ctn} where part_no='{i[0]}' and shelve='SHELVE'")
        print(f"CHECK SHELVE PARTNO: {i[0]}")
        
if __name__ == '__main__':
    p58()
    hold()
    stock()
    pgdb.commit()
    pgdb.close()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)