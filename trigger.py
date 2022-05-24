import sys
import os
import psycopg2 as pgsql
import cx_Oracle
from nanoid import generate
from spllibs import LogActivity as log
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
    try:
        sql = f"SELECT RECEIVINGKEY,RECPLNCTN,RECENDCTN,RECPLNCTN-RECENDCTN diff  FROM TXP_RECTRANSENT WHERE VENDOR='INJ' AND  RECEIVINGDTE >= (sysdate - 1)"
        obj = Oracur.execute(sql)
        for i in obj.fetchall():
            receive_no = str(i[0])
            receive_pln = int(str(i[1]))
            receive_rec = int(str(i[2]))
            receive_diff = int(str(i[3]))
            print(f"update receive_no {receive_no}")
            if receive_diff == 0:
                Oracur.execute(f"UPDATE TMP_RECTRANSENT SET RECENDCTN=RECPLNCTN WHERE MERGEID='{receive_no}'")
                Oracur.execute(f"UPDATE TMP_RECTRANSBODY SET RECCTN=PLNCTN  WHERE MERGEID='{receive_no}'")
                
            Oracon.commit()
            
    except Exception as ex:
        print(ex)
        pass
    
    Oracon.close()

if __name__ == '__main__':
    main()
    sys.exit(0)