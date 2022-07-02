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

def main():
    df = pd.read_excel("stocks/ck.xls", index_col=None)
    data = df.to_dict('records')
    r = 1
    for i in data:
        whs = "TAP"
        part_no = str(i["part_no"]).strip()
        lotno = int(str(i["lot_no"]).strip())
        serial_no = str(i["serial_no"]).strip()
        die_no = str(i["die_no"]).strip()
        revision_no = "-"##str().strip()
        qty = str(i["qty"]).strip()
        ctn = 1 ## str().strip()
        spl_ctn = 0 ##str().strip()
        is_matched = 'false'
        ch = Oracur.execute(f"SELECT RUNNINGNO,PARTNO FROM TXP_STKTAKECARTON WHERE RUNNINGNO='{serial_no}' AND STKTAKECHKFLG=1")
        db = ch.fetchone()
        if db != None:
            is_matched = 'true'
            spl_ctn = 1
            part_no = str(db[1]).strip()
            
        pg_cursor.execute(f"select serial_no from tbt_carton_checks where serial_no='{serial_no}'")
        if pg_cursor.fetchone() is None:
            sql = f"""insert into tbt_carton_checks (whs, part_no, lotno, serial_no, dieno, revision_no, qty, ctn, spl_ctn, is_matched, lastupdate)values('TAP', '{part_no}', '{lotno}', '{serial_no}', '{die_no}', '{revision_no}', {qty}, {ctn}, {spl_ctn}, {is_matched}, current_timestamp)"""
            pg_cursor.execute(sql)
        
        if is_matched == 'false':
            print(is_matched)
            
        print(f"{r}. {part_no} ::=> check TAP serial no: {serial_no} matched: {is_matched}")
        r += 1
        
def sync_spl():
    sql = f"SELECT ts.PARTNO,tc.LOTNO,ts.RUNNINGNO,tc.CASEID,'-' revision_code,ts.STOCKQUANTITY,1 ctn  FROM TXP_STKTAKECARTON ts LEFT JOIN TXP_CARTONDETAILS tc ON ts.RUNNINGNO=tc.RUNNINGNO"
    data = Oracur.execute(sql)
    r = 1
    for i in data.fetchall():
        part_no = str(i[0]).strip()
        lot_no = str(i[1]).strip()
        serial_no = str(i[2]).strip()
        die_no = str(i[3]).strip()
        revision_no = str(i[4]).strip()
        qty = str(i[5]).strip()
        spl_ctn = 1
        is_matched = 'false'
        pg_cursor.execute(f"select serial_no from tbt_carton_checks where serial_no='{serial_no}'")
        db = pg_cursor.fetchone()
        sql_insert = f"""insert into tbt_carton_checks (whs, part_no, lotno, serial_no, dieno, revision_no, qty, ctn, spl_ctn, is_matched, lastupdate)values('SPL', '{part_no}', '{lot_no}', '{serial_no}', '{die_no}', '{revision_no}', {qty}, 0, {spl_ctn}, false, current_timestamp)"""
        if db != None:
            is_matched = 'true'
            sql_insert = f"""update tbt_carton_checks set part_no='{part_no}',lotno='{lot_no}',dieno='{die_no}',revision_no='{revision_no}',qty={qty},spl_ctn=1,is_matched=true,lastupdate=current_timestamp where serial_no='{serial_no}'"""
        
        pg_cursor.execute(sql_insert)
        print(f"{r}. {part_no} ::=> check SPL serial no: {serial_no} matched: {is_matched}")
        r += 1
            
if __name__ == '__main__':
    main()
    sync_spl()
    Oracon.commit()
    pgdb.commit()
    pgdb.close()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)