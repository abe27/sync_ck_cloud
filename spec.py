from datetime import datetime
import shutil
import sys
import os
import time
import psycopg2 as pgsql
import cx_Oracle
from nanoid import generate
from spllibs import Yazaki, SplApi, SplSharePoint, LogActivity as log
from dotenv import load_dotenv
load_dotenv()

SERVICE_TYPE="CK2"
YAZAKI_HOST=f"https://{os.environ.get('HOST')}:{os.environ.get('PORT')}"
YAZAKI_USER=os.environ.get('CK_USERNAME')
YAZAKI_PASSWORD=os.environ.get('CK_PASSWORD')

SPL_API_HOST=os.environ.get('SPL_SITE_URL')
SPL_API_USERNAME=os.environ.get('SPL_USERNAME')
SPL_API_PASSWORD=os.environ.get('SPL_PASSWORD')

SHAREPOINT_SITE_URL=os.environ.get('SHAREPOINT_URL')
SHAREPOINT_SITE_NAME=os.environ.get('SHAREPOINT_URL_SITE')
SHAREPOINT_USERNAME=os.environ.get('SHAREPOINT_USERNAME')
SHAREPOINT_PASSWORD=os.environ.get('SHAREPOINT_PASSWORD')

DB_HOSTNAME=os.environ.get('DATABASE_URL')
DB_PORT=os.environ.get('DATABASE_PORT')
DB_NAME=os.environ.get('DATABASE_NAME')
DB_USERNAME=os.environ.get('DATABASE_USERNAME')
DB_PASSWORD=os.environ.get('DATABASE_PASSWORD')

ORA_DNS=f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME=os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD=os.environ.get('ORAC_DB_PASSWORD')

def main():
    Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
    Oracur = Oracon.cursor()
    log(name='TRIGGER', subject="UPDATE DATA", status="Success", message=f"Start Service")
    try:
        rnd = 1
        db = Oracur.execute(f"SELECT INVOICENO,PARTNO,RVMANAGINGNO,count(*) ctn,RECEIVINGQUANTITY*count(*)  FROM TXP_CARTONDETAILS  GROUP BY INVOICENO,PARTNO,RVMANAGINGNO,RECEIVINGQUANTITY ORDER BY INVOICENO,PARTNO,RVMANAGINGNO")
        stock = db.fetchall()
        for i in stock:
            rec_no = str(i[0])
            part_no = str(i[1])
            rvm_no = str(i[2])
            ctn = float(str(i[3]))
            qty = float(str(i[4]))
            sql = f"UPDATE TXP_RECTRANSBODY SET RVMANAGINGNO='{rvm_no}',RECCTN='{ctn}',RECQTY='{qty}'  WHERE  RECEIVINGKEY='{rec_no}' AND PARTNO='{part_no}'"
            Oracur.execute(sql)
            print(sql)
                
        Oracon.commit()
    except Exception as ex:
        log(name='TRIGGER', subject="UPDATE DATA", status="Error", message=str(ex))
        pass
    
    Oracon.close()
    log(name='TRIGGER', subject="UPDATE DATA", status="Success", message=f"End Service")
    
def update_receive():
    sql =f"""select 'SI22051400001' recno, p.no,sum(d.plan_ctn) pln  from tbt_receives t
    inner join tbt_receive_details d on t.id=d.receive_id
    inner join tbt_ledgers l on d.ledger_id=l.id 
    inner join tbt_parts p on l.part_id=p.id 
    where t.receive_no in ('TI2022051401', 'TI2022051402', 'TI2022051403')
    group by p.no
    order by p.no"""
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
    Oracur = Oracon.cursor()
    pln = 0
    for i in mycursor.fetchall():
        sql_part = f"UPDATE TXP_RECTRANSBODY SET PLNCTN='{str(i[2])}' WHERE RECEIVINGKEY='{str(i[0])}' AND PARTNO='{str(i[1])}'"
        Oracur.execute(sql_part)
        pln += float(str(i[2]))
    
    sql_update = f"UPDATE TXP_RECTRANSENT SET RECPLNCTN='{pln}',RECENDCTN=(SELECT sum(RECCTN) FROM TXP_RECTRANSBODY WHERE RECEIVINGKEY ='{str(i[0])}') WHERE RECEIVINGKEY='{str(i[0])}'"
    Oracur.execute(sql_update)
    print(sql_update)
    Oracon.commit() 
    Oracon.close()  
    mydb.commit()
    mydb.close()

if __name__ == '__main__':
    update_receive()
    sys.exit(0)