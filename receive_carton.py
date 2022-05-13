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
        log(name='CARTON', subject="Start", status="Success", message=f"Sync Receive")    
        mycursor = mydb.cursor()
        mycursor.execute("""select d.id,t.receive_date,t.receive_no,substring(t.receive_no, 11, 2) rnd,p."no",p."name",d.plan_qty,d.plan_ctn,case when c.ctn is null then 0 else c.ctn end,l.id ledger_id from tbt_receives t
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
            ledger_id = str(i[9])
            print(f"CHECK RECEIVE :=> {receive_no} PART: {part_no}")
            ora_sql = f"""SELECT '{receive_body_id}' rec_id,e.RECEIVINGDTE,t.RECEIVINGKEY,c.PARTNO,c.RVMANAGINGNO,c.LOTNO,c.RUNNINGNO,c.CASEID dieno,c.CASENO division,c.STOCKQUANTITY,t.OLDERKEY,c.SHELVE,c.PALLETKEY FROM TXP_RECTRANSBODY t 
            INNER JOIN TXP_RECTRANSENT e ON t.RECEIVINGKEY = e.RECEIVINGKEY 
            INNER JOIN TXP_CARTONDETAILS c ON t.RVMANAGINGNO = c.RVMANAGINGNO  AND t.PARTNO = c.PARTNO  
            WHERE c.PARTNO='{part_no}' AND TO_CHAR(e.RECEIVINGDTE, 'YYYY-MM-DD') = '{receive_date}' AND IS_CHECK=0  AND t.OLDERKEY LIKE '%{rnd}%'
            ORDER BY c.RUNNINGNO
            FETCH FIRST {plan_ctn} ROWS ONLY"""
            # ora_sql = f"""SELECT '{receive_body_id}' rec_id,e.RECEIVINGDTE,t.RECEIVINGKEY,c.PARTNO,c.RVMANAGINGNO,c.LOTNO,c.RUNNINGNO,c.CASEID dieno,c.CASENO division,c.STOCKQUANTITY,t.OLDERKEY,c.SHELVE,c.PALLETKEY FROM TXP_RECTRANSBODY t 
            # INNER JOIN TXP_RECTRANSENT e ON t.RECEIVINGKEY = e.RECEIVINGKEY 
            # INNER JOIN TXP_CARTONDETAILS c ON t.RECEIVINGKEY = c.INVOICENO AND t.PARTNO = c.PARTNO 
            # WHERE c.PARTNO='{part_no}' AND TO_CHAR(e.RECEIVINGDTE, 'YYYY-MM-DD') = '{receive_date}' AND t.OLDERKEY LIKE '%{rnd}%'
            # ORDER BY c.RUNNINGNO
            # FETCH FIRST {plan_ctn} ROWS ONLY"""
            # print(ora_sql)
            rvm_no = None
            obj = Oracur.execute(ora_sql)
            for x in obj.fetchall():
                rvm_no = str(x[4])
                lotno = str(x[5])
                serial_no = str(x[6])
                die_no = str(x[7]).replace('None', '')
                division_no = str(x[8]).replace('None', '')
                std_pack = str(x[9])
                location_no = str(x[11]).replace('None', '-')
                pallet_no = str(x[12]).replace('None', '-')
                #### create carton on stock Cloud
                mycursor.execute(f"select id from tbt_cartons where serial_no='{serial_no}'")
                carton_id = generate(size=36)
                sql_carton = f"""insert into tbt_cartons(id, receive_detail_id, lot_no, serial_no, die_no, division_no, qty, is_active, created_at, updated_at)values('{carton_id}', '{receive_body_id}', '{lotno}', '{serial_no}', '{die_no}', '{division_no}', '{std_pack}', true, current_timestamp, current_timestamp)"""
                if mycursor.fetchone() is None:
                    #### check stock
                    mycursor.execute(f"select id from tbt_stocks where ledger_id='{ledger_id}'")
                    if mycursor.fetchone() is None:
                        stock_id = generate(size=36)
                        mycursor.execute(f"""insert into tbt_stocks(id, ledger_id, per_qty, ctn, is_active, created_at, updated_at)values('{stock_id}', '{ledger_id}', {std_pack}, 0, true, current_timestamp, current_timestamp)""")
                        
                    mycursor.execute(f"update tbt_stocks set per_qty='{std_pack}',ctn=(ctn + 1) where ledger_id='{ledger_id}'")
                    mycursor.execute(sql_carton)
                    
                #### update location
                mycursor.execute(f"select id from tbt_locations where name='{location_no}'")
                location_id =  mycursor.fetchone()[0]
                
                ### check part on shelve
                mycursor.execute(f"select id from tbt_cartons where serial_no='{serial_no}'")
                carton_id = mycursor.fetchone()[0]
                mycursor.execute(f"select id from tbt_shelves where carton_id='{carton_id}' and location_id='{location_id}'")
                sql_shelve = f"insert into tbt_shelves(id,carton_id,location_id,pallet_no,is_printed,is_active,created_at,updated_at)values('{generate(size=36)}','{carton_id}','{location_id}','{pallet_no}',false,true,current_timestamp,current_timestamp)"
                if mycursor.fetchone():
                    sql_shelve = f"update tbt_shelves set pallet_no='{pallet_no}',updated_at=current_timestamp where carton_id='{carton_id}' and location_id='{location_id}'"
                    
                mycursor.execute(sql_shelve)
                Oracur.execute(f"UPDATE TXP_CARTONDETAILS SET IS_CHECK=1 WHERE RUNNINGNO='{serial_no}'")
                print(f"RVM NO: {rvm_no} SERIAL NO: {serial_no}")
                
            #### update rvm no
            if rvm_no:
                log(name='CARTON', subject="SYNC RECEIVE", status="Success", message=f"Sync {receive_no} PART: {part_no} RVM NO: {rvm_no}")    
                sql_update_receive = f"update tbt_receive_details set managing_no='{rvm_no}',updated_at=current_timestamp where id='{receive_body_id}'"
                # print(sql_update_receive)
                mycursor.execute(sql_update_receive)
            
        Oracon.commit()    
        mydb.commit()
        log(name='CARTON', subject="END", status="Success", message=f"Sync Receive")    
    except Exception as ex:
        log(name='CARTON', subject="UPLOAD RECEIVE", status="Error", message=str(ex))
        Oracon.rollback()
        mydb.rollback()
        pass
    
    # Oracon.commit()
    Oracon.close()
    mydb.close()
    
# def update_master_location():
#     mydb = pgsql.connect(
#         host=DB_HOSTNAME,
#         port=DB_PORT,
#         user=DB_USERNAME,
#         password=DB_PASSWORD,
#         database=DB_NAME,
#     )
    
#     Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
#     Oracur = Oracon.cursor()
        
#     try:
#         log(name='MASTER', subject="Start", status="Success", message=f"Sync Shelve")    
#         mycursor = mydb.cursor()
#         mycursor.execute("""select t.id,t.die_no,t.serial_no  from tbt_cartons t where t.die_no='None' order by t.serial_no""")
        
#         data = mycursor.fetchall()
#         for i in data:
#             carton_id = str(i[0])
#             die_no = str(i[1])
#             serial_no = str(i[2])
#             ora_sql = f"""SELECT CASEID,CASENO  FROM TXP_CARTONDETAILS WHERE RUNNINGNO='{serial_no}'"""
#             obj = Oracur.execute(ora_sql)
#             for x in obj.fetchall():
#                 die_no = str(x[0])
#                 division_no = str(x[1]).replace('None', '')
#                 #### create carton on stock Cloud
#                 mycursor.execute(f"update tbt_cartons set die_no='{die_no}', division_no='{division_no}' where id='{carton_id}'")
                
#         mydb.commit()
#         log(name='MASTER', subject="END", status="Success", message=f"Sync Shelve")    
#     except Exception as ex:
#         log(name='MASTER', subject="UPLOAD SHELVE", status="Error", message=str(ex))
#         Oracon.rollback()
#         mydb.rollback()
#         pass
    
#     # Oracon.commit()
#     Oracon.close()
#     mydb.close()

def update_master_location():
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
            txt = "UPDATE"
            if mycursor.fetchone() is None:
                txt = "INSERT"
                shelve_id = generate(size=36)
                mycursor.execute(f"insert into tbt_locations(id, name, description, is_active, created_at, updated_at)values('{shelve_id}', '{str(x[0])}', '-', true, current_timestamp, current_timestamp)")
            
            print(f"{txt} {str(x[0])}")
        mydb.commit()    
    except Exception as ex:
        log(name='CARTON', subject="UPLOAD RECEIVE", status="Error", message=str(ex))
        pass
    
    # Oracon.commit()
    Oracon.close()
    mydb.close()
    
if __name__ == '__main__':
    main()
    update_master_location()
    sys.exit(0)