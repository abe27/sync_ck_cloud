from lib2to3.pgen2 import token
import os
import cx_Oracle
from typing import Union

from fastapi import FastAPI
from pydantic import BaseModel
from spllibs import SplApi,LogActivity as log

from dotenv import load_dotenv
load_dotenv()


SPL_API_HOST=os.environ.get('SPL_SITE_URL')
SPL_API_USERNAME=os.environ.get('SPL_USERNAME')
SPL_API_PASSWORD=os.environ.get('SPL_PASSWORD')

ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')

class Item(BaseModel):
    whs: str
    receive_type: str
    part_no: str
    serial_no: str


spl = SplApi(SPL_API_HOST, SPL_API_USERNAME, SPL_API_PASSWORD)
app = FastAPI()


@app.get('/')
async def get():
    return {
        "message": "Hello world"
    }


@app.post("/")
async def create_item(item: Item):
    token = None
    Oracon = cx_Oracle.connect(
        user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS)
    Oracur = Oracon.cursor()
    sql = f"""SELECT whs,factory,rec_date,invoice_no,rvmanagingno,part_type,part_no,unit,serial_no,lot_no,case_id,case_no,std_pack_qty,qty,shelve,pallet_no,on_stock_ctn,event_trigger,olderkey,siid FROM TBT_SERIAL_TRIGGER WHERE SERIAL_NO='{item.serial_no}' AND PART_NO='{item.part_no}'"""
    obj = Oracur.execute(sql)
    i = obj.fetchone()
    doc = {
        "whs" : item.whs,
        "factory" : i[1],
        "rec_date" : i[2],
        "invoice_no" : i[3],
        "rvmanagingno" : i[4],
        "part_type" : i[5],
        "part_no" : i[6],
        "unit" : i[7],
        "serial_no" : i[8],
        "lot_no" : i[9],
        "case_id" : i[10],
        "case_no" : i[11],
        "std_pack_qty" : float(i[12]),
        "qty" : float(i[13]),
        "shelve" : i[14],
        "pallet_no" : i[15],
        "on_stock_ctn" : float(i[16]),
        "event_trigger" : item.receive_type,
        "olderkey" : i[18],
        "emp_id" : i[19],
    }

    Oracon.close()
    # token = spl.login()
    # spl.serial_no_tracking(token, doc)
    # spl.logout(token)
    spl.serial_no_tracking(token, doc)
    log(name='API', subject='GET request', status='Success', message=f'Get Data {doc["serial_no"]}')
    return doc
