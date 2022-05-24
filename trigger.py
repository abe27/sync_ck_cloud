import sys
import os
import psycopg2 as pgsql
import cx_Oracle
from nanoid import generate
from spllibs import SplApi,LogActivity as log
from dotenv import load_dotenv
load_dotenv()

SERVICE_TYPE = "CK2"
YAZAKI_HOST = f"https://{os.environ.get('HOST')}:{os.environ.get('PORT')}"
YAZAKI_USER = os.environ.get('CK_USERNAME')
YAZAKI_PASSWORD = os.environ.get('CK_PASSWORD')

SPL_API_HOST = os.environ.get('SPL_SITE_URL')
SPL_API_USERNAME = os.environ.get('SPL_USERNAME')
SPL_API_PASSWORD = os.environ.get('SPL_PASSWORD')

SHAREPOINT_SITE_URL = os.environ.get('SHAREPOINT_URL')
SHAREPOINT_SITE_NAME = os.environ.get('SHAREPOINT_URL_SITE')
SHAREPOINT_USERNAME = os.environ.get('SHAREPOINT_USERNAME')
SHAREPOINT_PASSWORD = os.environ.get('SHAREPOINT_PASSWORD')

DB_HOSTNAME = os.environ.get('DATABASE_URL')
DB_PORT = os.environ.get('DATABASE_PORT')
DB_NAME = os.environ.get('DATABASE_NAME')
DB_USERNAME = os.environ.get('DATABASE_USERNAME')
DB_PASSWORD = os.environ.get('DATABASE_PASSWORD')

ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')

spl = SplApi(SPL_API_HOST, SPL_API_USERNAME, SPL_API_PASSWORD)

def main():
    Oracon = cx_Oracle.connect(
        user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS)
    Oracur = Oracon.cursor()
    try:
        sql = f"SELECT RECEIVINGKEY,MERGEID  FROM TMP_RECTRANSENT WHERE SYNC=0 AND RECPLNCTN = RECENDCTN"
        obj = Oracur.execute(sql)
        for i in obj.fetchall():
            receive_no = str(i[0])
            receive_merge = str(i[1]).strip()
            # print(f"update receive_no {receive_no}")
            sql_merge = f"SELECT PARTNO,RECCTN  FROM TMP_RECTRANSBODY WHERE RECEIVINGKEY='{receive_no}' AND MERGEID='{receive_merge}' ORDER BY PARTNO,RUNNINGNO"
            merge_data = Oracur.execute(sql_merge)
            for x in merge_data.fetchall():
                part_no = str(x[0])
                limit_ctn = int(str(x[1]))
                sql_carton = f"""SELECT LOTNO,RUNNINGNO,CASEID,RECEIVINGQUANTITY,SHELVE,CASE WHEN PALLETKEY IS NULL THEN '-' ELSE PALLETKEY END PALLETKEY,PALLETNO transferout,IS_CHECK  FROM TXP_CARTONDETAILS WHERE INVOICENO='{receive_merge}' AND PARTNO='{part_no}' AND IS_CHECK=0 ORDER BY LOTNO,RUNNINGNO FETCH FIRST {limit_ctn} ROWS ONLY"""
                sql_data_receive = Oracur.execute(sql_carton)
                doc = []
                for b in sql_data_receive.fetchall():
                    doc = {
                        "whs": "CK-2",
                        "factory": "INJ",
                        "receive_no": receive_no,
                        "part_no": part_no,
                        "lot_no": str(b[0]),
                        "serial_no": str(b[1]),
                        "case_id": str(b[2]),
                        "std_pack_qty": float(str(b[3])),
                        "shelve": str(b[4]),
                        "pallet_no": str(b[5]),
                        "transfer_out": str(b[6]),
                        "event_trigger": "R"
                    }
                    spl.update_receive_trigger(doc)
                    is_check = str(b[7])
                    Oracur.execute(f"UPDATE TXP_CARTONDETAILS SET IS_CHECK=1 WHERE RUNNINGNO='{str(b[1])}'")
                    print(b)

            Oracur.execute(f"DELETE FROM TMP_RECTRANSENT WHERE MERGEID='{receive_merge}'")
            Oracur.execute(f"DELETE FROM TMP_RECTRANSBODY WHERE MERGEID='{receive_merge}'")
        Oracon.commit()

    except Exception as ex:
        print(ex)
        pass

    Oracon.close()
    
def check_carton():
    Oracon = cx_Oracle.connect(
        user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS)
    Oracur = Oracon.cursor()
    try:
        sql_carton = f"""SELECT 'CK-2' whs, 'INJ' factory,INVOICENO,PARTNO,LOTNO,RUNNINGNO,CASE WHEN CASEID IS NULL THEN '-' ELSE CASEID END CASEID,RECEIVINGQUANTITY,SHELVE,CASE WHEN PALLETKEY IS NULL THEN '-' ELSE PALLETKEY END PALLETKEY,PALLETNO transferout,IS_CHECK  FROM TXP_CARTONDETAILS WHERE IS_CHECK=0 ORDER BY LOTNO,RUNNINGNO"""
        sql_data_receive = Oracur.execute(sql_carton)
        doc = []
        for b in sql_data_receive.fetchall():
            doc = {
                "whs": str(b[0]),
                "factory": str(b[1]),
                "receive_no": str(b[2]),
                "part_no": str(b[3]),
                "lot_no": str(b[4]),
                "serial_no": str(b[5]),
                "case_id": str(b[6]),
                "std_pack_qty": float(str(b[7])),
                "shelve": str(b[8]),
                "pallet_no": str(b[9]),
                "transfer_out": str(b[10]),
                "event_trigger": "R"
            }
            
            spl.update_receive_trigger(doc)
            Oracur.execute(f"UPDATE TXP_CARTONDETAILS SET IS_CHECK=1 WHERE RUNNINGNO='{str(b[5])}'")
            Oracon.commit()
            print(f"carton ==>b {str(b)}" )
    except Exception as ex:
        print(ex)
        pass
    
    Oracon.close()


if __name__ == '__main__':
    main()
    check_carton()
    sys.exit(0)
