import sys
import os
import cx_Oracle
import openpyxl as xl
import pandas as pd
from spllibs import Yazaki, SplApi, SplSharePoint, LogActivity as log
from os.path import join, dirname
from dotenv import load_dotenv
# dotenv_path = join(dirname(__file__), '.env')
# load_dotenv(dotenv_path)
load_dotenv()

ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')

pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME,
                             dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Oracur = Oracon.cursor()

def main():
    df = pd.read_excel("stocks/ck1_out.xlsx", index_col=None)
    data = df.to_dict('records')
    r = 1
    for i in data:
        shelve_no = str(i["SHELVE"]).strip()
        part_no = str(i["PARTNO"]).strip()
        serial_no = str(i["SERIAL"]).strip()
        lot_no = str(i["LOTNO"]).strip()
        
        txt = f"{serial_no} NOT UPDATE "
        if (shelve_no in ['S-CK1','S-PLOUT']):
            sql = f"UPDATE TXP_CARTONDETAILS SET RECLOCATE=SHELVE,SIDTE=UPDDTE,SINO='TIMVOUT',SIID='SKTSYS',STOCKQUANTITY=0,SHELVE='{shelve_no}'  WHERE PARTNO='{part_no}' AND LOTNO='{lot_no}' AND RUNNINGNO='{serial_no}'"
            txt = "UPDATE STOCK= 0"
            Oracur.execute(sql)
            Oracon.commit()
            
        print(f"{r}. {txt} {shelve_no}")
        r += 1
        
        
if __name__ == '__main__':
    main()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)