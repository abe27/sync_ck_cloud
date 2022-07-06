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

pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Oracur = Oracon.cursor()

def main():
    sql = f"UPDATE TXP_CARTONDETAILS SET SIDTE=NULL,SINO=NULL,SIID=NULL WHERE STOCKQUANTITY > 0 AND SIDTE IS NOT NULL"
    Oracur.execute(sql)

if __name__ == '__main__':
    main()
    Oracon.commit()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)