import os
import cx_Oracle
from typing import Union

from fastapi import FastAPI
from pydantic import BaseModel

from dotenv import load_dotenv
load_dotenv()


ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')


class Item(BaseModel):
    whs: str
    receive_type: str
    part_no: str
    serial_no: str


app = FastAPI()


@app.get('/')
async def get():
    return {
        "message": "Hello world"
    }


@app.post("/")
async def create_item(item: Item):
    Oracon = cx_Oracle.connect(
        user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS)
    Oracur = Oracon.cursor()
    sql = f"""SELECT '{item.whs}' whs,r.factory,r.rec_date,t.INVOICENO  invoice_no,t.RVMANAGINGNO,CASE WHEN substr(t.PARTNO, 1, 2) = '71' THEN 'PLATE' ELSE 'PART' END part_type,t.PARTNO part_no,'BOX' unit,t.RUNNINGNO serial_no,t.LOTNO lot_no,t.CASEID case_id,CASE WHEN t.CASENO IS NULL THEN 0 ELSE t.CASENO END case_no,t.RECEIVINGQUANTITY std_pack_qty,t.RECEIVINGQUANTITY qty,t.SHELVE shelve,CASE WHEN t.PALLETKEY IS NULL THEN '-' ELSE t.PALLETKEY END pallet_no,stk.on_stock on_stock_ctn,'{item.receive_type}' event_trigger,r.olderkey
    FROM TXP_CARTONDETAILS t
    LEFT JOIN (
        SELECT c.partno, count(c.partno) on_stock FROM TXP_CARTONDETAILS c WHERE c.shelve NOT IN ('S-PLOUT') GROUP BY c.partno
    ) stk ON t.PARTNO=stk.partno 
    LEFT JOIN (
        SELECT e.VENDOR factory,to_char(e.RECEIVINGDTE, 'YYYY-MM-DD') rec_date,b.RECEIVINGKEY,b.PARTNO,b.RVMANAGINGNO,b.whs,b.olderkey FROM TXP_RECTRANSBODY b 
        INNER JOIN TXP_RECTRANSENT e ON b.RECEIVINGKEY = e.RECEIVINGKEY 
        GROUP BY b.PARTNO,b.RECEIVINGKEY,b.RVMANAGINGNO,b.whs,b.olderkey,e.RECEIVINGDTE,e.VENDOR
    ) r ON t.INVOICENO = r.RECEIVINGKEY AND t.RVMANAGINGNO=r.RVMANAGINGNO AND t.PARTNO=r.PARTNO
    WHERE t.PARTNO='{item.part_no}' AND t.RUNNINGNO='{item.serial_no}'"""
    obj = Oracur.execute(sql)
    
    for i in obj.fetchall():
        print(i)

    Oracon.close()
    return item
