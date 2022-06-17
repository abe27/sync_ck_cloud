import os
import cx_Oracle
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:3000",
    "http://192.168.101.217:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from dotenv import load_dotenv
load_dotenv()

ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')

### Start Up
pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Oracur = Oracon.cursor()

@app.on_event("shutdown")
async def shut_down():
    print(f"api shutdown")
    pool.release(Oracon)
    Oracon.close()

@app.get('/')
async def get():
    return {
        "message": "Hello world"
    }
    
@app.get('/part')
async def get():
    sql = f"""SELECT a.ON_YEAR,a.ON_FIFO_MONTH,a.PARTNO,b.ctn,min(a.shelve) shelve
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
                        c.PARTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY
                    FROM TXP_CARTONDETAILS c 
                    WHERE c.SHELVE NOT IN ('S-XXX','S-PLOUT', 'S-HOLD', 'S-P59', 'S-P58', 'S-P57', 'S-CK1')
                    GROUP BY SUBSTR(c.LOTNO, 0, 1),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY
                    ORDER BY SUBSTR(c.LOTNO, 0, 1),SUBSTR(c.LOTNO, 2, 2),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE
                ) a
                INNER JOIN (
                    SELECT PARTNO,count(PARTNO) ctn
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
                            c.PARTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY
                        FROM TXP_CARTONDETAILS c 
                        WHERE c.SHELVE NOT IN ('S-XXX','S-PLOUT', 'S-HOLD', 'S-P59', 'S-P58', 'S-P57', 'S-CK1')
                        GROUP BY SUBSTR(c.LOTNO, 0, 1),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY
                        ORDER BY SUBSTR(c.LOTNO, 0, 1),SUBSTR(c.LOTNO, 2, 2),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE
                    )
                    GROUP BY PARTNO
                    ORDER BY PARTNO
                ) b ON a.PARTNO=b.PARTNO
                GROUP BY a.ON_YEAR,a.ON_FIFO_MONTH,a.PARTNO,b.ctn
                ORDER BY a.ON_YEAR,a.ON_FIFO_MONTH,a.PARTNO"""
    Oracur.execute(sql)
    obj = Oracur.fetchall()
    doc = []
    for r in obj:
        doc.append({
            "on_year": r[0],
            "on_fifo_month": str(r[1]).strip(),
            "part_no": str(r[2]),
            "ctn": str(r[3]),
            "min_shelve": str(r[4])
        })
    return doc

@app.get('/detail/{part_no}')
async def get(part_no):
    sql = f"""SELECT PARTNO,LOTNO,RUNNINGNO,SHELVE,STOCKQUANTITY
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
                c.PARTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY
            FROM TXP_CARTONDETAILS c 
            WHERE c.SHELVE NOT IN ('S-XXX','S-PLOUT', 'S-HOLD', 'S-P59', 'S-P58', 'S-P57', 'S-CK1')
            GROUP BY SUBSTR(c.LOTNO, 0, 1),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY
            ORDER BY SUBSTR(c.LOTNO, 0, 1),SUBSTR(c.LOTNO, 2, 2),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE
        )
        WHERE partno like '{part_no}%'
        ORDER BY ON_YEAR,ON_FIFO_MONTH,PARTNO,LOTNO,RUNNINGNO,SHELVE"""
    Oracur.execute(sql)
    obj = Oracur.fetchall()
    doc = []
    for r in obj:
        doc.append({
            "part_no": r[0],
            "lotno": str(r[1]).strip(),
            "serial_no": str(r[2]),
            "shelve": str(r[3]),
            "qty": float(str(r[4]))
        })
    return doc

@app.get('/shelve/{shelve_no}')
async def get(shelve_no):
    sql = f"""SELECT PARTNO,LOTNO,RUNNINGNO,SHELVE,STOCKQUANTITY
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
                c.PARTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY
            FROM TXP_CARTONDETAILS c 
            WHERE c.SHELVE NOT IN ('S-XXX','S-PLOUT', 'S-HOLD', 'S-P59', 'S-P58', 'S-P57', 'S-CK1')
            GROUP BY SUBSTR(c.LOTNO, 0, 1),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE,c.STOCKQUANTITY
            ORDER BY SUBSTR(c.LOTNO, 0, 1),SUBSTR(c.LOTNO, 2, 2),c.PARTNO,c.LOTNO,c.RUNNINGNO,c.SHELVE
        )
        WHERE shelve like '{shelve_no}%'
        ORDER BY ON_YEAR,ON_FIFO_MONTH,PARTNO,LOTNO,RUNNINGNO,SHELVE"""
    Oracur.execute(sql)
    obj = Oracur.fetchall()
    doc = []
    for r in obj:
        doc.append({
            "part_no": r[0],
            "lotno": str(r[1]).strip(),
            "serial_no": str(r[2]),
            "shelve": str(r[3]),
            "qty": float(str(r[4]))
        })
    return doc