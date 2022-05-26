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
            FETCH FIRST 6000 ROWS ONLY"""
    # print(sql)
    obj = Ora.execute(sql)
    
    part_stk = None
    i = 1
    for r in obj.fetchall():
        whs = str(r[0]).strip()
        factory = str(r[1]).strip()
        rec_date = str(r[2]).strip()
        invoice_no = str(r[3]).strip()
        rvmanagingno = str(r[4]).strip()
        part_type = str(r[5]).strip()
        part_no = str(r[6]).strip()
        part_name = str(r[7]).replace("'", "''")
        unit = str(r[8]).strip()
        serial_no = str(r[9]).strip()
        lot_no = str(r[10]).strip()
        case_id = str(r[11]).strip()
        case_no = int(str(r[12]))
        std_pack_qty = int(str(r[13]))
        qty = int(str(r[14]))
        shelve = str(r[15]).strip()
        pallet_no = str(r[16]).strip()
        on_stock = int(str(r[17]))
        on_stock_ctn = int(str(r[18]))
        event_trigger = str(r[19])
        older_key = str(r[20]).strip()
        siid = str(r[21]).strip()
        
        ### Get Master Data
        pg_cursor.execute(f"select id from tbt_tagrps where name='C'")
        tagrp_id = pg_cursor.fetchone()[0]
        
        pg_cursor.execute(f"select id from tbt_part_types where name='{part_type}'")
        part_type_id = pg_cursor.fetchone()[0]
        
        pg_cursor.execute(f"select id from tbt_whs where name='{whs}'")
        whs_id = pg_cursor.fetchone()[0]
        
        pg_cursor.execute(f"select id from tbt_factory_types where name='{factory}'")
        fac_id = pg_cursor.fetchone()[0]
        
        pg_cursor.execute(f"select id from tbt_units where name='{unit}'")
        unit_id = pg_cursor.fetchone()[0]
        
        pg_cursor.execute(f"select id from tbt_locations where name='{shelve}'")
        shelve_id = generate(size=36)
        sh = pg_cursor.fetchone()
        sql_shelve = f"insert into tbt_locations(id,name,description,is_active,created_at,updated_at)values('{shelve_id}','{shelve}','{shelve}',true,current_timestamp,current_timestamp)"
        if sh:
            shelve_id = sh[0]
            sql_shelve = f"update tbt_locations set updated_at=current_timestamp where id='{shelve_id}'"
            
        pg_cursor.execute(sql_shelve)

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
        
        ### check ledger
        pg_cursor.execute(f"select id from tbt_ledgers where part_type_id='{part_type_id}' and tagrp_id='{tagrp_id}' and factory_id='{fac_id}' and whs_id='{whs_id}' and part_id='{part_id}'")
        ledger = pg_cursor.fetchone()
        ledger_id = generate(size=36)
        sql_ledger = f"""insert into tbt_ledgers(id, part_type_id, tagrp_id, factory_id, whs_id, part_id, kinds_id, sizes_id, colors_id, width, length, height, net_weight, gross_weight, unit_id, is_active, created_at, updated_at)values('{ledger_id}', '{part_type_id}', '{tagrp_id}', '{fac_id}', '{whs_id}', '{part_id}', null, null, null, 0, 0, 0, 0, 0, '{unit_id}', true, current_timestamp, current_timestamp)"""
        if ledger:
            ledger_id = ledger[0]
            sql_ledger = f"update tbt_ledgers set updated_at=current_timestamp where id='{ledger_id}'"
            
        pg_cursor.execute(sql_ledger)
        
        ### check carton
        pg_cursor.execute(f"select id from tbt_cartons where ledger_id='{ledger_id}' and serial_no='{serial_no}'")
        carton = pg_cursor.fetchone()
        carton_id = generate(size=36)
        sql_carton = f"""insert into tbt_cartons(id,ledger_id,lot_no,serial_no,die_no,revision_no,qty,is_active,created_at,updated_at)values('{carton_id}','{ledger_id}','{lot_no}','{serial_no}','{case_id}','{case_no}','{qty}',true,current_timestamp,current_timestamp)"""
        if carton:
            carton_id = carton[0]
            sql_carton = f"""update tbt_cartons set ledger_id='{ledger_id}',lot_no='{lot_no}',serial_no='{serial_no}',die_no='{case_id}',revision_no='{case_no}',qty='{qty}',is_active=true,updated_at=current_timestamp where id='{carton_id}'"""
        pg_cursor.execute(sql_carton)
        
        ### check carton on shelve
        pg_cursor.execute(f"select id from tbt_shelves where carton_id='{carton_id}' and location_id='{shelve_id}'")
        carton_shelve = pg_cursor.fetchone()
        carton_shelve_id = generate(size=36)
        sql_shelve = f"insert into tbt_shelves(id, carton_id, location_id, pallet_no, is_printed, is_active, created_at, updated_at)values('{carton_shelve_id}', '{carton_id}', '{shelve_id}', '{pallet_no}', false, true, current_timestamp, current_timestamp)"
        if carton_shelve:
            carton_shelve_id = carton_shelve[0]
            sql_shelve = f"update tbt_shelves set location_id='{shelve_id}',pallet_no='{pallet_no}',is_printed=true,is_active=true,updated_at=current_timestamp where id='{carton_shelve_id}'"
        pg_cursor.execute(sql_shelve)
        
        ### check stock
        ctn = 1
        if qty == 0:ctn = 1
        pg_cursor.execute(f"select id from tbt_stocks where ledger_id='{ledger_id}'")
        stock = pg_cursor.fetchone()
        stock_id = generate(size=36)
        sql_stock = f"""insert into tbt_stocks(id, ledger_id, per_qty, ctn, is_active, created_at, updated_at)values('{stock_id}', '{ledger_id}', {std_pack_qty}, {ctn}, true, current_timestamp, current_timestamp)"""
        if stock:
            stock_id = stock[0]
            sql_stock = f"""update tbt_stocks set ctn=ctn+{ctn},is_active=true,updated_at=current_timestamp where id='{stock_id}'"""
        pg_cursor.execute(sql_stock)
        
        Ora.execute(f"UPDATE TXP_CARTONDETAILS SET IS_CHECK=1 WHERE RUNNINGNO='{serial_no}'")
        print(f"{i} ==> part: {part_no} serial no: {serial_no} qty: {qty} ctn: {ctn}")
        i += 1
        
    pgdb.commit()
    pgdb.close()

if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        Conn.rollback()
        pass
    
    Conn.commit()
    pool.release(Conn)
    pool.close()
    sys.exit(0)
