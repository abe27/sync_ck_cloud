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
    
class ReceiveEnt(BaseModel):
    receive_no: str
    plan_ctn: int
    rec_ctn: int


spl = SplApi(SPL_API_HOST, SPL_API_USERNAME, SPL_API_PASSWORD)
app = FastAPI()

### Start Up
pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Oracur = Oracon.cursor()

@app.on_event("shutdown")
async def shut_down():
    print(f"api shutdown")
    pool.release()
    Oracon.close()

@app.get('/')
async def get():
    return {
        "message": "Hello world"
    }


@app.get('/receive/key/{receive_id}')
async def get_receive_receive_key(receive_id):
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        RABBITMQ_HOST, RABBITMQ_PORT, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='receive_data')
    Oracur.execute(
        f"SELECT RECEIVINGKEY,RECPLNCTN,RECENDCTN,RECPLNCTN-RECENDCTN diff FROM TXP_RECTRANSENT WHERE RECEIVINGKEY='{receive_id}'")
    obj = Oracur.fetchone()
    doc = {
        "receive_no": obj[0],
        "plan_ctn": float(str(obj[1])),
        "rec_ctn": float(str(obj[2])),
        "diff_ctn": float(str(obj[3]))
    }

    channel.basic_publish(
        exchange='', routing_key='receive_data', body=json.dumps(doc))
    print(" [x] Sent 'Hello World!'")
    connection.close()
    return {
        "message": "Send Hello world"
    }


@app.get('/receive/get/{factory}')
async def get_receive_factory(factory):
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        RABBITMQ_HOST, RABBITMQ_PORT, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='receive_data')
    Oracur.execute(
        f"SELECT RECEIVINGKEY,RECPLNCTN,RECENDCTN,RECPLNCTN-RECENDCTN diff FROM TXP_RECTRANSENT WHERE to_char(RECEIVINGDTE, 'YYYYMMDD') = TO_CHAR(sysdate - 0, 'YYYYMMDD') AND VENDOR='{factory}' order by RECEIVINGDTE")
    obj = Oracur.fetchall()
    doc = []
    for i in obj:
        doc.append({
            "receive_no": i[0],
            "plan_ctn": float(str(i[1])),
            "rec_ctn": float(str(i[2])),
            "diff_ctn": float(str(i[3]))
        })

    channel.basic_publish(
        exchange='', routing_key='receive_data', body=json.dumps(doc))
    print(f"[x] Sent 'Receive Data({len(doc)})!'")
    connection.close()
    return {
        "message": f"Send Receive Data",
        "data": doc
    }


@app.post("/")
async def create_item(item: Item):
    token = None
    sql = f"""SELECT '{item.whs}' whs,CASE WHEN substr(t.INVOICENO, 1, 2) = 'TI' THEN 'INJ' ELSE 'AW' END factory,TO_CHAR(TO_DATE(SUBSTR(t.INVOICENO, 3,6), 'YYMMDD'), 'YYYY-MM-DD')  rec_date,t.INVOICENO  invoice_no,t.RVMANAGINGNO,CASE WHEN substr(t.PARTNO, 1, 2) = '71' THEN 'PLATE' ELSE 'PART' END part_type,t.PARTNO part_no,'BOX' unit,t.RUNNINGNO serial_no,t.LOTNO lot_no,t.CASEID case_id,CASE WHEN t.CASENO IS NULL THEN 0 ELSE t.CASENO END case_no,t.RECEIVINGQUANTITY std_pack_qty,t.RECEIVINGQUANTITY qty,t.SHELVE shelve,CASE WHEN t.PALLETKEY IS NULL THEN '-' ELSE t.PALLETKEY END pallet_no,0 on_stock, 0 on_stock_ctn,'R' event_trigger,'-' olderkey,CASE WHEN t.SIID IS NULL THEN 'NO' ELSE t.SIID END SIID
            FROM TXP_CARTONDETAILS t
            WHERE t.PARTNO='{item.part_no}' AND t.RUNNINGNO='{item.serial_no}'"""
    # print(sql)
    log(name='API_QUERY', subject='GET', status='Success', message=f'PART: {item.part_no} SERIAL: {item.serial_no}')
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
        "on_stock": float(i[16]),
        "on_stock_ctn": float(i[17]),
        "event_trigger": i[18],
        "olderkey": i[19],
        "emp_id": i[20],
    }
    
    # print(doc)
    spl.serial_no_tracking(token, doc)
    log(name='API', subject='GET request', status='Success',
        message=f'Get Data {doc["serial_no"]}')
    return doc

@app.post('/receive/trigger')
async def  receive_trigger(recData: ReceiveEnt):
    # print(recData)
    diff = recData.plan_ctn - recData.rec_ctn
    if diff == 0:
        Oracur.execute(f"UPDATE TMP_RECTRANSENT SET RECENDCTN=RECPLNCTN WHERE MERGEID='{recData.receive_no}'")
        Oracur.execute(f"UPDATE TMP_RECTRANSBODY SET RECCTN=PLNCTN  WHERE MERGEID='{recData.receive_no}'")
        Oracon.commit()
    return (recData)
    