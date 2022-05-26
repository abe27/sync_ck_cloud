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

def main():
    ### Initail PostgreSQL Server
    pgdb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    pg_cursor = pgdb.cursor()
    sql = f"""SELECT 'CK-2' whs,CASE WHEN substr(t.PARTNO, 1, 1) = '1' THEN 'AW' ELSE 'INJ' END factory,to_char(t.SYSDTE, 'YYYY-MM-DD')  rec_date,t.INVOICENO  invoice_no,t.RVMANAGINGNO,CASE WHEN p.TYPE IS NULL THEN '-' ELSE CASE WHEN p.TYPE = 'PRESS' THEN 'PLATE' ELSE p.TYPE END END  part_type,t.PARTNO part_no,p.PARTNAME,'BOX' unit,t.RUNNINGNO serial_no,t.LOTNO lot_no,t.CASEID case_id,CASE WHEN t.CASENO IS NULL THEN 0 ELSE t.CASENO END case_no,t.RECEIVINGQUANTITY std_pack_qty,t.RECEIVINGQUANTITY qty,t.SHELVE shelve,CASE WHEN t.PALLETKEY IS NULL THEN '-' ELSE t.PALLETKEY END pallet_no,0 on_stock, 0 on_stock_ctn,'R' event_trigger,'-' olderkey,CASE WHEN t.SIID IS NULL THEN 'NO' ELSE t.SIID END SIID
            FROM TXP_CARTONDETAILS t
            INNER JOIN TXP_PART p ON t.PARTNO=p.PARTNO  
            WHERE t.IS_CHECK=0
            ORDER BY t.PARTNO,t.LOTNO,t.RUNNINGNO 
            FETCH FIRST 1000 ROWS ONLY"""
    # print(sql)
    obj = Ora.execute(sql)
    
    part_stk = None
    i = 1
    for r in obj.fetchall():
        whs = str(r[0])
        factory = str(r[1])
        rec_date = str(r[2])
        invoice_no = str(r[3]).strip()
        rvmanagingno = str(r[4])
        part_type = str(r[5])
        part_no = str(r[6])
        part_name = str(r[7]).replace("'", "''")
        unit = str(r[8])
        serial_no = str(r[9])
        lot_no = str(r[10])
        case_id = str(r[11])
        case_no = int(str(r[12]))
        std_pack_qty = int(str(r[13]))
        qty = int(str(r[14]))
        shelve = str(r[15])
        pallet_no = str(r[16])
        on_stock = int(str(r[17]))
        on_stock_ctn = int(str(r[18]))
        event_trigger = str(r[19])
        older_key = str(r[20])
        siid = str(r[21])
        
        ### Get Master Data
        

        ### check stock
        if part_stk is None: part_stk = part_no
        if (part_no == part_stk) is False:part_stk = part_no
        
        ### check part
        pg_cursor.execute(f"select id from tbt_parts where no='{part_stk}'")
        part = pg_cursor.fetchone()
        part_id = generate(size=36)
        sql_part = "insert into tbt_parts(id, no,name, is_active,created_at,updated_at)values('{part_id}','{part_stk}','{part_name}',true,current_timestamp,current_timestamp)"
        if part:
            part_id = part[0]
            sql_part = "update tbt_parts set updated_at=current_timestamp where id = '{part_id}'"
            
        pg_cursor.execute(sql_part)
        
        
        
        print(f"{i} ==> part: {part_no} serial no: {serial_no}")
        i += 1

    pgdb.close()

if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        pass
    
    Conn.commit()
    pool.release(Conn)
    pool.close()
    sys.exit(0)
