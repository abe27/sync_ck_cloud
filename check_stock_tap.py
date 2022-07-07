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

def ins(whs, shelve, i):
    part_no = str(i["part_no"])
    qty = int(i["qty"])
    ctn = int(i["ctn"])
    
    sql = f"select part_no,qty,ctn from tbt_stocks where part_no='{part_no}' and shelve='{shelve}'"
    pg_cursor.execute(sql)
    pg = pg_cursor.fetchone()
    txt = f"UPDATE {part_no}"
    _qty = qty
    _ctn = ctn
    # if pg != None:
    #     _qty = int(pg[1])
    #     _ctn = int(pg[2])
    
    sql_insert = f"update tbt_stocks set whs='{whs}',qty={_qty},ctn={_ctn} where part_no='{part_no}' and shelve='{shelve}'"
    if pg is None:
        txt = f"INSERT {part_no}"
        sql_insert = f"""insert into tbt_stocks(whs, part_no, qty, ctn, created_at, shelve, spl_check)values('{whs}', '{part_no}', {_qty}, {_ctn}, current_timestamp, '{shelve}', 0)"""
    
    pg_cursor.execute(sql_insert)
    print(txt + f" QTY: {_qty} CTN: {_ctn} SHELVE: {shelve} whs: {whs}")
    
def update_stk_spl(whs, shelve, i):
    part_no = str(i[0])
    qty = int(i[1])
    ctn = int(i[2])
    
    sql = f"select part_no,qty,spl_check from tbt_stocks where part_no='{part_no}' and shelve='{shelve}'"
    pg_cursor.execute(sql)
    pg = pg_cursor.fetchone()
    txt = f"UPDATE {part_no}"
    _qty = qty
    _ctn = ctn
    sql_insert = f"update tbt_stocks set matched='1',qty={_qty},spl_check={_ctn} where part_no='{part_no}' and shelve='{shelve}'"
    if pg is None:
        txt = f"INSERT {part_no}"
        sql_insert = f"""insert into tbt_stocks(whs, part_no, qty, ctn, created_at, shelve, spl_check)values('{whs}', '{part_no}', {_qty}, 0, current_timestamp, '{shelve}', {_ctn})"""
    
    pg_cursor.execute(sql_insert)
    print(txt + f" QTY: {_qty} CTN: {_ctn} SHELVE: {shelve} whs: {whs}")

def check_stock(sql):
    # print(sql)
    ctn = Oracur.execute(sql)
    return int(ctn.fetchone()[0])

def get_data(serial_no):
    shelve = Oracur.execute(f"SELECT SHELVE FROM TXP_STKTAKECARTON ts WHERE ts.RUNNINGNO='{serial_no}'")
    if shelve == None:
        return "SNON"
    
    return shelve.fetchone()[0]
        
def stock():
    # df = pd.read_excel("stocks/new_tap_stock.xlsx", index_col=None)
    # data = df.to_dict('records')
    # for i in data:
    #     ins('TAP', 'SHELVE', i)
        
    # pgdb.commit()
    
    sql = f"select part_no,shelve,spl_check from tbt_stocks where shelve='SHELVE'"
    pg_cursor.execute(sql)
    db = pg_cursor.fetchall()
    for i in db:
        check_ctn = check_stock(f"SELECT count(PARTNO) FROM TXP_STKTAKECARTON WHERE PARTNO='{i[0]}' AND STKTAKECHKFLG IS NOT NULL AND SHELVE NOT IN ('S-PLOUT', 'S-CK1', 'S-XXX')")
        recheck_ctn = check_stock(f"SELECT count(PARTNO) FROM TXP_STKTAKECARTON WHERE PARTNO='{i[0]}' AND STKTAKECHKFLG IS NULL AND SHELVE IN ('S-RECHECK')")
        current_stock = check_stock(f"SELECT count(PARTNO) FROM TXP_CARTONDETAILS WHERE PARTNO='{i[0]}' AND SHELVE NOT IN ('S-PLOUT', 'S-CK1', 'S-XXX')")
        pg_cursor.execute(f"update tbt_stocks set spl_check={check_ctn},current_stock='{current_stock}',recheck_ctn='{recheck_ctn}' where part_no='{i[0]}' and shelve='SHELVE' and whs='TAP'")
        print(f"CHECK SHELVE PARTNO: {i[0]}")
        
    pgdb.commit()
        
def get_stock():
    sql = f"SELECT PARTNO,RECEIVINGQUANTITY qty,count(partno) ctn FROM TXP_STKTAKECARTON WHERE STKTAKECHKFLG IS NOT NULL GROUP BY PARTNO,RECEIVINGQUANTITY ORDER BY PARTNO"
    data = Oracur.execute(sql)
    for i in data:
        update_stk_spl('SPL', 'SHELVE', i)
        
    pgdb.commit()
        
if __name__ == '__main__':
    stock()
    get_stock()
    Oracon.commit()
    pgdb.close()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)