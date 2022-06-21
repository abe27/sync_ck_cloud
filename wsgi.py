import json
from dotenv import load_dotenv
import os
import cx_Oracle
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

load_dotenv()

ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')

# Start Up
pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Oracur = Oracon.cursor()


@app.route('/')
def home():
    return "Hello World"

@app.get('/detail/<part_no>')
# @cross_origin()
async def detail(part_no):
    sql = f"""SELECT ON_YEAR,ON_FIFO_MONTH,PARTNO,LOTNO,RUNNINGNO,SHELVE,STOCKQUANTITY,PALLETKEY
        FROM (
            SELECT 
                CASE 
                    WHEN SUBSTR(c.LOTNO, 0, 1) > 2 THEN '201'||SUBSTR(c.LOTNO, 0, 1)
                ELSE
                    '202'||SUBSTR(c.LOTNO, 0, 1) 
                END on_year,
                CASE 
                    WHEN SUBSTR(c.LOTNO, 0, 1) > 2 THEN '201'||SUBSTR(c.LOTNO, 0, 1)
                ELSE
                    '202'||SUBSTR(c.LOTNO, 0, 1) 
                END  || SUBSTR(c.LOTNO, 2, 2) ON_FIFO_MONTH,
                c.LOTNO,
                c.PARTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY,c.PALLETKEY
            FROM TXP_CARTONDETAILS c 
            WHERE c.SHELVE NOT IN ('S-XXX','S-PLOUT')
            GROUP BY SUBSTR(c.LOTNO, 0, 1),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY,c.PALLETKEY
            ORDER BY SUBSTR(c.LOTNO, 0, 1),SUBSTR(c.LOTNO, 2, 2),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE
        )
        WHERE partno ='{part_no}'
        ORDER BY ON_YEAR,ON_FIFO_MONTH,PARTNO,LOTNO,RUNNINGNO,SHELVE"""
    Oracur.execute(sql)
    obj = Oracur.fetchall()
    doc = []
    for r in obj:
        doc.append({
            "on_year": str(r[0]).strip(),
            "on_fifo_month": str(r[1]).strip(),
            "part_no": str(r[2]).strip(),
            "lotno": str(r[3]).strip(),
            "serial_no": str(r[4]),
            "shelve": str(r[5]),
            "qty": float(str(r[6])),
            "pallet_no": str(r[7])
        })
        
    response  = jsonify(doc)  
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.get('/shelve/<shelve_name>')
# @cross_origin()
async def shelve(shelve_name):
    sql = f"""SELECT ON_YEAR,ON_FIFO_MONTH,PARTNO,LOTNO,RUNNINGNO,SHELVE,STOCKQUANTITY,PALLETKEY
        FROM (
            SELECT 
                CASE 
                    WHEN SUBSTR(c.LOTNO, 0, 1) > 2 THEN '201'||SUBSTR(c.LOTNO, 0, 1)
                ELSE
                    '202'||SUBSTR(c.LOTNO, 0, 1) 
                END on_year,
                CASE 
                    WHEN SUBSTR(c.LOTNO, 0, 1) > 2 THEN '201'||SUBSTR(c.LOTNO, 0, 1)
                ELSE
                    '202'||SUBSTR(c.LOTNO, 0, 1) 
                END  || SUBSTR(c.LOTNO, 2, 2) ON_FIFO_MONTH,
                c.LOTNO,
                c.PARTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY,c.PALLETKEY
            FROM TXP_CARTONDETAILS c
            GROUP BY SUBSTR(c.LOTNO, 0, 1),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY,c.PALLETKEY
            ORDER BY SUBSTR(c.LOTNO, 0, 1),SUBSTR(c.LOTNO, 2, 2),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE
        )
        WHERE SHELVE ='{shelve_name}'
        ORDER BY ON_YEAR,ON_FIFO_MONTH,PARTNO,LOTNO,RUNNINGNO,SHELVE"""
    if shelve_name == "SNON":
        sql = f"""SELECT ON_YEAR,ON_FIFO_MONTH,PARTNO,LOTNO,RUNNINGNO,SHELVE,STOCKQUANTITY,PALLETKEY,UPDDTE
        FROM (
            SELECT 
                CASE 
                    WHEN SUBSTR(c.LOTNO, 0, 1) > 2 THEN '201'||SUBSTR(c.LOTNO, 0, 1)
                ELSE
                    '202'||SUBSTR(c.LOTNO, 0, 1) 
                END on_year,
                CASE 
                    WHEN SUBSTR(c.LOTNO, 0, 1) > 2 THEN '201'||SUBSTR(c.LOTNO, 0, 1)
                ELSE
                    '202'||SUBSTR(c.LOTNO, 0, 1) 
                END  || SUBSTR(c.LOTNO, 2, 2) ON_FIFO_MONTH,
                c.LOTNO,
                c.PARTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY,c.PALLETKEY
            FROM TXP_CARTONDETAILS c
            GROUP BY SUBSTR(c.LOTNO, 0, 1),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY,c.PALLETKEY,c.UPDDTE 
            ORDER BY SUBSTR(c.LOTNO, 0, 1),SUBSTR(c.LOTNO, 2, 2),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE
        )
        WHERE SHELVE ='{shelve_name}' AND TO_CHAR(UPDDTE, 'YYYYMMDD')  <= TO_CHAR(sysdate - 1, 'YYYYMMDD') 
        ORDER BY ON_YEAR,ON_FIFO_MONTH,PARTNO,LOTNO,RUNNINGNO,SHELVE"""
    
    Oracur.execute(sql)
    obj = Oracur.fetchall()
    doc = []
    for r in obj:
        doc.append({
            "on_year": str(r[0]).strip(),
            "on_fifo_month": str(r[1]).strip(),
            "part_no": str(r[2]).strip(),
            "lotno": str(r[3]).strip(),
            "serial_no": str(r[4]),
            "shelve": str(r[5]),
            "qty": float(str(r[6])),
            "pallet_no": str(r[7])
        })
        
    response  = jsonify(doc)  
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5050, debug=True)