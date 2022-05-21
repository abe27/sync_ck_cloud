from lib2to3.pgen2 import token
import os
import cx_Oracle
from typing import Union
import pika
import json

from fastapi import FastAPI
from pydantic import BaseModel
from spllibs import SplApi, LogActivity as log

from dotenv import load_dotenv
load_dotenv()


SPL_API_HOST = os.environ.get('SPL_SITE_URL')
SPL_API_USERNAME = os.environ.get('SPL_USERNAME')
SPL_API_PASSWORD = os.environ.get('SPL_PASSWORD')

ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = os.environ.get('RABBITMQ_PORT')
RABBITMQ_USERNAME = os.environ.get('RABBITMQ_USERNAME')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')


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


@app.get('/receive/{receive_id}')
async def get(receive_id):
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='receive_data')
    Oracon = cx_Oracle.connect(
        user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS)
    Oracur = Oracon.cursor()
    Oracur.execute(
        f"SELECT RECEIVINGKEY,RECPLNCTN,RECENDCTN,RECPLNCTN-RECENDCTN diff FROM TXP_RECTRANSENT WHERE RECEIVINGKEY='{receive_id}'")
    obj = Oracur.fetchone()
    doc = {
        "receive_no":obj[0],
        "plan_ctn": float(str(obj[1])),
        "rec_ctn": float(str(obj[2])),
        "diff_ctn": float(str(obj[3]))
        }

    channel.basic_publish(
        exchange='', routing_key='receive_data', body=json.dumps(doc))
    print(" [x] Sent 'Hello World!'")
    connection.close()
    Oracon.close()
    return {
        "message": "Send Hello world"
    }


@app.post("/")
async def create_item(item: Item):
    token = None
    Oracon = cx_Oracle.connect(
        user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS)
    Oracur = Oracon.cursor()
    sql = f"""SELECT '{item.whs}' whs,r.factory,r.rec_date,CASE WHEN bb.RECEIVINGKEY IS NULL THEN t.INVOICENO ELSE bb.RECEIVINGKEY END invoice_no,t.RVMANAGINGNO,CASE WHEN substr(t.PARTNO, 1, 2) = '71' THEN 'PLATE' ELSE 'PART' END part_type,t.PARTNO part_no,'BOX' unit,t.RUNNINGNO serial_no,t.LOTNO lot_no,t.CASEID case_id,CASE WHEN t.CASENO IS NULL THEN 0 ELSE t.CASENO END case_no,t.RECEIVINGQUANTITY std_pack_qty,t.RECEIVINGQUANTITY qty,t.SHELVE shelve,CASE WHEN t.PALLETKEY IS NULL THEN '-' ELSE t.PALLETKEY END pallet_no,stk.on_stock on_stock_ctn,'{item.receive_type}' event_trigger,r.olderkey,CASE WHEN t.SIID IS NULL THEN 'NO' ELSE t.SIID END SIID
    FROM TXP_CARTONDETAILS t
    LEFT JOIN (
        SELECT c.partno, count(c.partno) on_stock FROM TXP_CARTONDETAILS c WHERE c.shelve NOT IN ('S-PLOUT') GROUP BY c.partno
    ) stk ON t.PARTNO=stk.partno 
    LEFT JOIN (
        SELECT b.MERGEID,b.PARTNO,b.PLNCTN,b.RECEIVINGKEY,b.RUNNINGNO  FROM TMP_RECTRANSBODY b
    ) bb ON t.INVOICENO = bb.MERGEID AND t.PARTNO=bb.PARTNO AND t.RVMANAGINGNO=bb.RUNNINGNO
    LEFT JOIN (
        SELECT e.VENDOR factory,to_char(e.RECEIVINGDTE, 'YYYY-MM-DD') rec_date,b.RECEIVINGKEY,b.PARTNO,b.RVMANAGINGNO,b.whs,b.olderkey FROM TXP_RECTRANSBODY b 
        INNER JOIN TXP_RECTRANSENT e ON b.RECEIVINGKEY = e.RECEIVINGKEY 
        GROUP BY b.PARTNO,b.RECEIVINGKEY,b.RVMANAGINGNO,b.whs,b.olderkey,e.RECEIVINGDTE,e.VENDOR
    ) r ON t.INVOICENO = r.RECEIVINGKEY AND t.RVMANAGINGNO=r.RVMANAGINGNO AND t.PARTNO=r.PARTNO
    WHERE t.PARTNO='{item.part_no}' AND t.RUNNINGNO='{item.serial_no}'"""
    obj = Oracur.execute(sql)
    i = obj.fetchone()
    doc = {
        "whs": i[0],
        "factory": i[1],
        "rec_date": i[2],
        "invoice_no": i[3],
        "rvmanagingno": i[4],
        "part_type": i[5],
        "part_no": i[6],
        "unit": i[7],
        "serial_no": i[8],
        "lot_no": i[9],
        "case_id": i[10],
        "case_no": i[11],
        "std_pack_qty": float(i[12]),
        "qty": float(i[13]),
        "shelve": i[14],
        "pallet_no": i[15],
        "on_stock_ctn": float(i[16]),
        "event_trigger": i[17],
        "olderkey": i[18],
        "emp_id": i[19],
    }

    Oracon.close()
    # token = spl.login()
    # spl.serial_no_tracking(token, doc)
    # spl.logout(token)
    spl.serial_no_tracking(token, doc)
    log(name='API', subject='GET request', status='Success',
        message=f'Get Data {doc["serial_no"]}')
    return doc
