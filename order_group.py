from datetime import datetime
import shutil
import sys
import os
import time
import psycopg2 as pgsql
import cx_Oracle
from nanoid import generate
from dotenv import load_dotenv
load_dotenv()

def main():
    ### Initail Mysql Server
    mydb = pgsql.connect(
        host=192.168.101.217,
        port=5432,
        user="postgres",
        password="admin@spl",
        database="dbtest"
    )
    
    mycursor = mydb.cursor()
    sql = f"select vendor,bishpc,shiptype,pono from tbt_order_plans where vendor='INJ' group by vendor,bishpc,shiptype,pono order by vendor,bishpc,shiptype,pono"
    mycursor.execute(sql)
    for i in mycursor.fetchall():
        print(i)
        
    mydb.commit()
    mydb.close()
    
if __name__ == '__main__':
    main()
    sys.exit(0)
    