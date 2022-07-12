import sys
import os
import cx_Oracle
import pandas as pd
import psycopg2 as pgsql
from datetime import datetime
from nanoid import generate
from dotenv import load_dotenv
load_dotenv()

DB_HOSTNAME = os.environ.get('DATABASE_URL')
DB_PORT = os.environ.get('DATABASE_PORT')
DB_NAME = os.environ.get('DATABASE_NAME')
DB_USERNAME = os.environ.get('DATABASE_USERNAME')
DB_PASSWORD = os.environ.get('DATABASE_PASSWORD')

ORA_DNS=f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME=os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD=os.environ.get('ORAC_DB_PASSWORD')

### Initail Data

pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Oracur = Oracon.cursor()

### Initail PostgreSQL Server
pgdb = pgsql.connect(
    host=DB_HOSTNAME,
    port=DB_PORT,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    database=DB_NAME,
)
pg_cursor = pgdb.cursor()

def check_nan(txt):
    if txt == 'nan':return ''
    return txt

def read_invoice(target_dir, file_name):
    file_list_file = f"{target_dir}/{file_name}"
    try:
        df = pd.read_excel(file_list_file, index_col=None)
        data = df.to_dict('records')
        for i in data:
            fticket = check_nan(str(i['fticket']).strip())
            serial_no = check_nan(str(i['serialno']).strip())
            # partno = check_nan(str(i['partno']).strip())
            lotno = check_nan(str(i['lotno']).strip())
            sql_plout = f"SELECT CASE WHEN PALLETKEY IS NULL THEN '-' ELSE PALLETKEY END FROM TXP_CARTONDETAILS WHERE LOTNO='{lotno}' AND RUNNINGNO='{serial_no}'"
            Oracur.execute(sql_plout)
            db = Oracur.fetchone()
            plout = "-"
            if db != None:
                plout = db[0]
            
            sql_get_invoice_id = f"SELECT ISSUINGKEY FROM TXP_ISSPACKDETAIL WHERE FTICKETNO='{fticket}'"
            Oracur.execute(sql_get_invoice_id)
            ref_inv_id = Oracur.fetchone()[0]
            sql_update_invoice_id = f"UPDATE TXP_CARTONDETAILS SET SINO='{ref_inv_id}' WHERE LOTNO='{lotno}' AND RUNNINGNO='{serial_no}'"
            Oracur.execute(sql_update_invoice_id)
            
            sql_plout = f"UPDATE TXP_ISSPACKDETAIL SET CTNSN='{serial_no}',PLOUTNO='{plout}',UPDDTE=CURRENT_TIMESTAMP WHERE FTICKETNO='{fticket}'"
            Oracur.execute(sql_plout)
            print(f"UPDATE {fticket} SET {serial_no}")
        
        Oracon.commit()   
            
    except Exception as e:
        print(str(e))
        
def main():
    target_dir = os.path.join(os.path.dirname(__file__), "Invoice")
    list_file = os.listdir(target_dir)
    i = 0
    while i < len(list_file):
        read_invoice(target_dir, list_file[i])
        i += 1

if __name__ == '__main__':
    main()
    Oracon.commit()
    pgdb.commit()
    pgdb.close()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)