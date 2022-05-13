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

DB_HOSTNAME=os.environ.get('DATABASE_URL')
DB_PORT=os.environ.get('DATABASE_PORT')
DB_NAME=os.environ.get('DATABASE_NAME')
DB_USERNAME=os.environ.get('DATABASE_USERNAME')
DB_PASSWORD=os.environ.get('DATABASE_PASSWORD')

ORA_DNS=f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME=os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD=os.environ.get('ORAC_DB_PASSWORD')

# def update_master_location():
#     mydb = pgsql.connect(
#         host=DB_HOSTNAME,
#         port=DB_PORT,
#         user=DB_USERNAME,
#         password=DB_PASSWORD,
#         database=DB_NAME,
#     )
    
#     Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
#     Oracur = Oracon.cursor()
        
#     try:
#         log(name='MASTER', subject="Start", status="Success", message=f"Sync Shelve")    
#         mycursor = mydb.cursor()
#         mycursor.execute("""select t.id,t.die_no,t.serial_no  from tbt_cartons t where t.die_no='None' order by t.serial_no""")
        
#         data = mycursor.fetchall()
#         for i in data:
#             carton_id = str(i[0])
#             die_no = str(i[1])
#             serial_no = str(i[2])
#             ora_sql = f"""SELECT CASEID,CASENO  FROM TXP_CARTONDETAILS WHERE RUNNINGNO='{serial_no}'"""
#             obj = Oracur.execute(ora_sql)
#             for x in obj.fetchall():
#                 die_no = str(x[0])
#                 division_no = str(x[1]).replace('None', '')
#                 #### create carton on stock Cloud
#                 mycursor.execute(f"update tbt_cartons set die_no='{die_no}', division_no='{division_no}' where id='{carton_id}'")
                
#         mydb.commit()
#         log(name='MASTER', subject="END", status="Success", message=f"Sync Shelve")    
#     except Exception as ex:
#         log(name='MASTER', subject="UPLOAD SHELVE", status="Error", message=str(ex))
#         Oracon.rollback()
#         mydb.rollback()
#         pass
    
#     # Oracon.commit()
#     Oracon.close()
#     mydb.close()

def update_master_location():
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    mycursor = mydb.cursor()
    Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
    Oracur = Oracon.cursor()
    
    try:
        sql = f"SELECT SHELVE  FROM TXP_CARTONDETAILS GROUP BY SHELVE ORDER BY SHELVE"
        obj = Oracur.execute(sql)
        for x in obj.fetchall():
            mycursor.execute(f"select id from tbt_locations where name='{str(x[0])}'")
            # txt = "UPDATE"
            if mycursor.fetchone() is None:
                # txt = "INSERT"
                shelve_id = generate(size=36)
                mycursor.execute(f"insert into tbt_locations(id, name, description, is_active, created_at, updated_at)values('{shelve_id}', '{str(x[0])}', '-', true, current_timestamp, current_timestamp)")
            
            # print(f"{txt} {str(x[0])}")
        mydb.commit()    
    except Exception as ex:
        log(name='CARTON', subject="UPLOAD RECEIVE", status="Error", message=str(ex))
        pass
    
    # Oracon.commit()
    Oracon.close()
    mydb.close()
    
if __name__ == '__main__':
    update_master_location()
    sys.exit(0)