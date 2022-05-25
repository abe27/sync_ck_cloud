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
Oracon = pool.acquire()
# Oracon = cx_Oracle.connect(user=ORA_PASSWORD, password=ORA_USERNAME,dsn=ORA_DNS)
Oracur = Oracon.cursor()


async def serial_no_tracking(obj=[]):
    async with aiohttp.ClientSession() as session:
        url = f"{SPL_API_HOST}/trigger/carton"
        payload = json.dumps(obj)
        headers = {
            'Content-Type': 'application/json'
        }
        async with session.post(url, headers=headers, data=payload) as res:
            s = await res.json()
            print(s)
            session.close()
        # requests.request("POST", url, headers=headers, data=payload)

    return True


async def main():
    sql = f"SELECT INVOICENO,PARTNO,LOTNO,RUNNINGNO,RVMANAGINGNO,STOCKQUANTITY,SHELVE,CASE WHEN PALLETKEY IS NULL THEN '-' ELSE PALLETKEY END PALLETKEY,CASE WHEN CASEID IS NULL THEN '-' ELSE CASEID END CASE_ID,PALLETNO transfer_out,RECEIVINGQUANTITY  FROM TXP_CARTONDETAILS WHERE IS_CHECK=0 ORDER BY PARTNO,RUNNINGNO"
    obj = Oracur.execute(sql)
    i = 1
    for r in obj.fetchall():
        receive_no = str(r[0])
        part_no = str(r[1])
        lot_no = str(r[2])
        serial_no = str(r[3])
        rvm_no = str(r[4])
        qty = int(r[5])
        shelve = str(r[6])
        pallet_key = str(r[7])
        case_id = str(r[8])
        transfer_out = str(r[9])
        stdpack = float(r[10])

        ctn = 0
        if qty > 0:
            ctn = 1

        async with aiohttp.ClientSession() as session:
            url = f"{SPL_API_HOST}/trigger/carton"
            payload = json.dumps({
                "whs": "CK-2",
                "factory": "INJ",
                "invoice_no": receive_no,
                "part_no": part_no,
                "serial_no": serial_no,
                "lot_no": lot_no,
                "case_id": case_id,
                "stdpack": stdpack,
                "qty": qty,
                "ctn": ctn,
                "shelve": shelve,
                "pallet_no": pallet_key
            })
            headers = {
                'Content-Type': 'application/json'
            }
            async with session.post(url, headers=headers, data=payload) as res:
                # s = await res.json()
                # print(s)
                print(
                    f"{i} :==> {receive_no} part: {part_no} serial: {serial_no} qty: {qty} ctn: {ctn}")
                
        Oracur.execute( f"UPDATE TXP_CARTONDETAILS SET IS_CHECK=1 WHERE RUNNINGNO='{serial_no}'")
        i += 1


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as ex:
        print(ex)
        pass
    
    Oracon.commit()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)
