from datetime import datetime
import shelve
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

def main():
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
            if mycursor.fetchone() is None:
                print(f"insert {str(x[0])}")
                shelve_id = generate(size=36)
                mycursor.execute(f"insert into tbt_locations(id, name, description, is_active, created_at, updated_at)values('{shelve_id}', '{str(x[0])}', '-', true, current_timestamp, current_timestamp)")
        
        mydb.commit()    
    except Exception as ex:
        log(name='CARTON', subject="UPLOAD RECEIVE", status="Error", message=str(ex))
        pass
    
    # Oracon.commit()
    Oracon.close()
    mydb.close()

if __name__ == '__main__':
    main()
    sys.exit(0)