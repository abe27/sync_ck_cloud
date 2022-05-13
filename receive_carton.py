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

def main():
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    
    Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
    Oracur = Oracon.cursor()
        
    try:
        mycursor = mydb.cursor()
        mycursor.execute("""select d.id,t.receive_date,t.receive_no,substring(t.receive_no, 11, 2) rnd,p."no",p."name",d.plan_qty,d.plan_ctn,case when c.ctn is null then 0 else c.ctn end from tbt_receives t
        inner join tbt_receive_details d on t.id = d.receive_id
        inner join tbt_ledgers l on d.ledger_id = l.id
        inner join tbt_parts p on l.part_id = p.id
        left join (
            select cc.receive_detail_id,count(cc.receive_detail_id) ctn from tbt_cartons cc group by cc.receive_detail_id
        ) c on d.id = c.receive_detail_id 
        where t.receive_date >= (current_date - 1) and d.plan_ctn > (case when c.ctn is null then 0 else c.ctn end) and t.receive_no like 'TI%'
        order by t.receive_date,t.receive_no,p.no,p.name""")
        
        data = mycursor.fetchall()
        for i in data:
            receive_body_id = str(i[0])
            receive_date = str(i[1])
            receive_no = str(i[2])
            rnd = str(i[3])
            part_no = str(i[4])
            plan_ctn = str(i[7])
            print(f":=> {receive_no} PART: {part_no}")
            ora_sql = f"""SELECT '{receive_body_id}' rec_id,e.RECEIVINGDTE,t.RECEIVINGKEY,c.PARTNO,c.RVMANAGINGNO,c.LOTNO,c.RUNNINGNO,'' dieno,'' division,c.STOCKQUANTITY,t.OLDERKEY FROM TXP_RECTRANSBODY t 
            INNER JOIN TXP_RECTRANSENT e ON t.RECEIVINGKEY = e.RECEIVINGKEY 
            INNER JOIN TXP_CARTONDETAILS c ON t.RECEIVINGKEY = c.INVOICENO AND t.PARTNO = c.PARTNO 
            WHERE c.PARTNO='{part_no}' AND TO_CHAR(e.RECEIVINGDTE, 'YYYY-MM-DD') = '{receive_date}' AND IS_CHECK=0  AND t.OLDERKEY LIKE '%{rnd}%'
            ORDER BY c.RUNNINGNO
            FETCH FIRST {plan_ctn} ROWS ONLY"""
            # print(ora_sql)
            obj = Oracur.execute(ora_sql)
            for x in obj.fetchall():
                rvm_no = str(x[4])
                lotno = str(x[5])
                serial_no = str(x[6])
                die_no = str(x[7])
                division_no = str(x[8])
                std_pack = str(x[9])
                #### create carton on stock Cloud
                mycursor.execute(f"select id from tbt_cartons where serial_no='{serial_no}'")
                carton_id = generate(size=36)
                sql_carton = f"""insert into tbt_cartons(id, receive_detail_id, lot_no, serial_no, die_no, division_no, qty, is_active, created_at, updated_at)values('{carton_id}', '{receive_body_id}', '{lotno}', '{serial_no}', '{die_no}', '{division_no}', '{std_pack}', true, current_timestamp, current_timestamp)"""
                if mycursor.fetchone() is None:
                    # print(f"insert {serial_no}")
                    mycursor.execute(sql_carton)
                    
                Oracur.execute(f"UPDATE TXP_CARTONDETAILS SET IS_CHECK=1 WHERE RUNNINGNO='{serial_no}'")
                Oracon.commit()
                print(f"RVM NO: {rvm_no} SERIAL NO: {serial_no}")
                
            #### update rvm no
            sql_update_receive = f"update tbt_receive_details set managing_no='{rvm_no}',updated_at=current_timestamp where id='{receive_body_id}'"
            # print(sql_update_receive)
            mycursor.execute(sql_update_receive)
            mydb.commit()
        
    except Exception as ex:
        log(name='SPL', subject="UPLOAD RECEIVE", status="Error", message=str(ex))
        pass
    
    # Oracon.commit()
    Oracon.close()
    mydb.close()

if __name__ == '__main__':
    main()
    sys.exit(0)