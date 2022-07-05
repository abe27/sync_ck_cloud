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

### Initail Variables ###
stock_may = []

def load_stock_may():
    docs = []
    df = pd.read_excel("stocks/Stock_202205.xlsx", index_col=None)
    data = df.to_dict('records')
    for i in data:
        docs.append({
            "part_no": str(i["PARTNO"]).strip(),
            "ctn": int(str(i["CTN"]).strip())
        })
        
    return docs

def check_stock(part_no):
    stock_may = load_stock_may()
    i = 0
    while i < len(stock_may):
        r = stock_may[i]
        if r["part_no"] == part_no:
            return r["ctn"]
        
        i += 1
    return 0

def main():
    fname = f"stocks/result_check_stock.xlsx"
    file = xl.load_workbook(fname)
    sheet = file.worksheets[0]
    n = 3
    for row in sheet['C3:C555']:
        for i in row: 
            part_no = str(i.value)
            ### stock on MAY
            may = check_stock(part_no)
            ### check receive june
            sql_receive = f"SELECT count(PARTNO) ctn FROM TXP_CARTONDETAILS WHERE PARTNO='{part_no}' AND SYSDTE BETWEEN to_date('2022-05-31 12:00:00', 'YYYY-MM-DD HH24:MI:SS') AND to_date('2022-06-30 12:00:00', 'YYYY-MM-DD HH24:MI:SS')"
            rec = Oracur.execute(sql_receive)
            rec_ctn = int(str(rec.fetchone()[0]))
            ### check s-out june
            sql_out = f"SELECT count(PARTNO) ctn FROM TXP_CARTONDETAILS WHERE PARTNO='{part_no}' AND SHELVE='S-PLOUT' AND SYSDTE BETWEEN to_date('2022-05-31 12:00:00', 'YYYY-MM-DD HH24:MI:SS') AND to_date('2022-06-30 12:00:00', 'YYYY-MM-DD HH24:MI:SS')"
            plout = Oracur.execute(sql_out)
            plout_ctn = int(str(plout.fetchone()[0]))
            sheet[f"G{n}"].value = may
            sheet[f"H{n}"].value = rec_ctn
            sheet[f"I{n}"].value = plout_ctn
            print(f"C{n}. PART NO: {part_no} STOCK MAY: {may} RECEIVE: {rec_ctn} PLOUT: {plout_ctn}")
            n += 1
            
    file.save(fname)

if __name__ == '__main__':
    main()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)