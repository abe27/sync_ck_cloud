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
        db = Oracur.execute(f"SELECT UUID_KEY,SERIALNO,LASTUPDATE FROM TMP_SERIALTRACKING ORDER BY LASTUPDATE FETCH FIRST 1000 ROWS ONLY")
        stock = db.fetchall()
        if stock:
            mydb = pgsql.connect(
                host=DB_HOSTNAME,
                port=DB_PORT,
                user=DB_USERNAME,
                password=DB_PASSWORD,
                database=DB_NAME,
            )
            mycursor = mydb.cursor()
            for s in stock:
                uuid = str(s[0])
                serial_no = str(s[1])
                sql = f"SELECT CASE WHEN PALLETKEY IS NULL THEN '-' ELSE PALLETKEY END plkey,CASE WHEN CASEID IS NULL THEN '-' ELSE CASEID END CASEID,SHELVE,STOCKQUANTITY  FROM TXP_CARTONDETAILS WHERE RUNNINGNO='{serial_no}'"
                obj = Oracur.execute(sql)
                
                txt = "UPDATE"
                for i in obj.fetchall():
                    pl_no = str(i[0])
                    dv_no = str(i[1])
                    shelve_no = str(i[2])
                    on_stock = float(str(i[3]))
                    on_stock_ctn = float(str(i[3]))
                    
                    ### get tbt_cartons
                    mycursor.execute(f"select c.id,d.ledger_id,c.serial_no from tbt_cartons c inner join tbt_receive_details d on c.receive_detail_id = d.id where c.serial_no='{serial_no}'")
                    carton = mycursor.fetchone()
                    txt = f"NOT UPDATE {serial_no}"
                    if carton:
                        carton_id = carton[0]
                        ledger_id = carton[1]
                        
                        ### get location_id
                        mycursor.execute(f"select id from tbt_locations where name='{shelve_no}'")
                        location_id = mycursor.fetchone()[0]
                        
                        ### update shelve
                        mycursor.execute(f"select id from tbt_shelves where carton_id='{carton_id}' and location_id='{location_id}'")
                        shelve = mycursor.fetchone()
                        if shelve:
                            mycursor.execute(f"update tbt_shelves set location_id='{location_id}',pallet_no='{pl_no}',updated_at=current_timestamp  where id='{shelve[0]}'")
                            
                        mycursor.execute(f"update tbt_cartons set qty='{on_stock}',updated_at=current_timestamp  where id='{carton_id}'")
                        part = Oracur.execute(f"SELECT count(*) FROM TXP_CARTONDETAILS WHERE PARTNO=(SELECT PARTNO  FROM TXP_CARTONDETAILS WHERE RUNNINGNO='{serial_no}') AND SHELVE NOT IN ('S-PLOUT')")
                        on_stock_ctn = part.fetchone()[0]
                        mycursor.execute(f"update tbt_stocks set ctn='{on_stock_ctn}',updated_at=current_timestamp  where ledger_id='{ledger_id}'")
                        txt = f"UPDATE {serial_no} STOCK: {on_stock_ctn}"
                        
                    Oracur.execute(f"DELETE FROM TMP_SERIALTRACKING WHERE UUID_KEY='{uuid}'")
                    
                log(name='TRIGGER', subject="UPDATE DATA", status="Success", message=f"{rnd}. Sync {txt} ID: {uuid}")
                print(f"{rnd}. Sync {txt} ID: {uuid}")
                rnd += 1
                
            mydb.commit()    
            mydb.close()
            Oracon.commit()
    except Exception as ex:
        log(name='TRIGGER', subject="UPDATE DATA", status="Error", message=str(ex))
        pass
    
    Oracon.close()
    log(name='TRIGGER', subject="UPDATE DATA", status="Success", message=f"End Service")

if __name__ == '__main__':
    main()
    sys.exit(0)