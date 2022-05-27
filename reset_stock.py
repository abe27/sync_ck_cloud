import sys
import os
import cx_Oracle
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

ORA_DNS=f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME=os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD=os.environ.get('ORAC_DB_PASSWORD')

pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Ora = Oracon.cursor()

def main():
    df = pd.read_excel("./Data/reset_sxx.xlsx")
    for r in df['SERIAL_NO']:
        serial_no = str(r).strip()
        Ora.execute(f"DELETE FROM TXP_CARTONDETAILS WHERE RUNNINGNO='{serial_no}'")
    
    Oracon.commit()

if __name__ == '__main__':
    main()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)