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

# Initial Data
pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Conn = pool.acquire()
Ora = Conn.cursor()

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
        mycursor.execute("""select d.id,t.receive_date,t.receive_no,substring(t.receive_no, 11, 2) rnd,p.no,p.name,d.plan_qty,d.plan_ctn,case when tc.ctn is null then 0 else tc.ctn end ctn,l.id ledger_id,tfg.batch_id
        from tbt_receives t
        inner join tbt_receive_details d on t.id = d.receive_id
        inner join tbt_ledgers l on d.ledger_id = l.id
        inner join tbt_parts p on l.part_id = p.id
        inner join tbt_file_gedis tfg on t.file_gedi_id=tfg.id
        left join (select c.ledger_id,count(c.ledger_id) ctn from tbt_cartons c group by c.ledger_id) tc on d.ledger_id=tc.ledger_id 
        where d.plan_ctn > (case when tc.ctn is null then 0 else tc.ctn end) and t.receive_no like 'TI%'
        order by t.receive_date,t.receive_no,p.no,p.name""")
        
        rnd_x = 1
        data = mycursor.fetchall()
        for i in data:
            receive_body_id = str(i[0]).strip()
            receive_date = str(i[1]).strip()
            receive_no = str(i[2]).strip()
            rnd = str(i[3]).strip()
            part_no = str(i[4]).strip()
            plan_ctn = float(str(i[7]))
            diff_plan_ctn = float(str(i[8]))
            ledger_id = str(i[9]).strip()
            batch_id = str(i[10]).strip()
            
            # ora_sql = f"""SELECT '{receive_body_id}' rec_id,e.RECEIVINGDTE,t.RECEIVINGKEY,c.PARTNO,c.RVMANAGINGNO,c.LOTNO,c.RUNNINGNO,c.CASEID dieno,c.CASENO division,c.STOCKQUANTITY,t.OLDERKEY,c.SHELVE,c.PALLETKEY FROM TXP_RECTRANSBODY t 
            # INNER JOIN TXP_RECTRANSENT e ON t.RECEIVINGKEY = e.RECEIVINGKEY 
            # INNER JOIN TXP_CARTONDETAILS c ON t.RECEIVINGKEY = c.INVOICENO AND t.PARTNO = c.PARTNO 
            # WHERE c.PARTNO='{part_no}' AND TO_CHAR(e.RECEIVINGDTE, 'YYYY-MM-DD') = '{receive_date}' AND IS_CHECK=0  AND t.OLDERKEY LIKE '%{rnd}%'
            # ORDER BY c.RUNNINGNO
            # FETCH FIRST {plan_ctn - diff_plan_ctn} ROWS ONLY"""
            ora_sql = f"""SELECT '{receive_body_id}' rec_id,e.RECEIVINGDTE,t.RECEIVINGKEY,c.PARTNO,c.RVMANAGINGNO,c.LOTNO,c.RUNNINGNO,c.CASEID dieno,c.CASENO division,c.STOCKQUANTITY,t.OLDERKEY,c.SHELVE,c.PALLETKEY FROM TXP_RECTRANSBODY t 
            INNER JOIN TXP_RECTRANSENT e ON t.RECEIVINGKEY = e.RECEIVINGKEY 
            INNER JOIN TXP_CARTONDETAILS c ON t.RECEIVINGKEY = c.INVOICENO AND t.PARTNO = c.PARTNO 
            WHERE c.PARTNO='{part_no}' AND e.GEDI_FILE='{batch_id}'
            ORDER BY c.RUNNINGNO
            FETCH FIRST {plan_ctn - diff_plan_ctn} ROWS ONLY"""
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
                sql_carton = f"""insert into tbt_cartons(id, ledger_id, lot_no, serial_no, die_no, revision_no, qty, is_active, created_at, updated_at)values('{carton_id}', '{ledger_id}', '{lotno}', '{serial_no}', '{die_no}', '{division_no}', '{std_pack}', true, current_timestamp, current_timestamp)"""
                if mycursor.fetchone() is None:
                    #### check stock
                    mycursor.execute(f"select id from tbt_stocks where ledger_id='{ledger_id}'")
                    if mycursor.fetchone() is None:
                        stock_id = generate(size=36)
                        mycursor.execute(f"""insert into tbt_stocks(id, ledger_id, per_qty, ctn, is_active, created_at, updated_at)values('{stock_id}', '{ledger_id}', {std_pack}, 0, true, current_timestamp, current_timestamp)""")
                    
                    
                    ### get stock
                    stock_check = Oracur.execute(f"SELECT count(*) FROM  TXP_CARTONDETAILS WHERE PARTNO='{part_no}' AND SHELVE NOT IN ('S-PLOUT')")
                    ctn_stock = stock_check.fetchone()[0]
                    sql_update_stock = f"update tbt_stocks set per_qty='{std_pack}',ctn={ctn_stock},updated_at=current_timestamp where ledger_id='{ledger_id}'"
                    if int(std_pack) > 0:   
                        sql_update_stock = f"update tbt_stocks set ctn={ctn_stock},updated_at=current_timestamp where ledger_id='{ledger_id}'"
                    
                    mycursor.execute(sql_update_stock) 
                    
                else:
                    sql_carton = f"update tbt_cartons set ledger_id='{ledger_id}',updated_at=current_timestamp where serial_no='{serial_no}'"
                    
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
                # Oracur.execute(f"UPDATE TXP_CARTONDETAILS SET IS_CHECK=1 WHERE RUNNINGNO='{serial_no}'")
                print(f"RVM NO: {rvm_no} SERIAL NO: {serial_no}")
                
            #### update rvm no
            if rvm_no:
                log(name='CARTON', subject="SYNC RECEIVE", status="Success", message=f"Sync {receive_no} PART: {part_no} RVM NO: {rvm_no}")    
                sql_update_receive = f"update tbt_receive_details set managing_no='{rvm_no}',updated_at=current_timestamp where id='{receive_body_id}'"
                # print(sql_update_receive)
                mycursor.execute(sql_update_receive)
                print(f"{rnd_x} :=> {receive_no} update part: {part_no} rvm: {rvm_no} ctn: {plan_ctn}") 
                
            ### commit data
            # Oracon.commit()    
            mydb.commit()
            rnd_x += 1 # increment
            
        log(name='CARTON', subject="END", status="Success", message=f"Sync Receive")
    except Exception as ex:
        log(name='CARTON-ERROR', subject="UPLOAD RECEIVE", status="Error", message=str(ex))
        Oracon.rollback()
        mydb.rollback()
        pass
    
    # Oracon.commit()
    Oracon.close()
    mydb.close()
    
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
            # txt = "UPDATE"
            if mycursor.fetchone() is None:
                # txt = "INSERT"
                shelve_id = generate(size=36)
                mycursor.execute(f"insert into tbt_locations(id, name, description, is_active, created_at, updated_at)values('{shelve_id}', '{str(x[0])}', '-', true, current_timestamp, current_timestamp)")
            
            # print(f"{txt} {str(x[0])}")
        mydb.commit()    
    except Exception as ex:
        log(name='CARTON', subject="UPLOAD RECEIVE", status="Error", message=str(ex))
        pass
    
    # Oracon.commit()
    Oracon.close()
    mydb.close()
    
def get_receive():
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    mycursor = mydb.cursor()
    try:
        sql = f"""select d.id,g.batch_id,t.receive_no,p.no,d.plan_ctn,case when tc.ctn is null then 0 else tc.ctn end rec_ctn,l.id from tbt_receives t 
        inner join tbt_file_gedis g on t.file_gedi_id=g.id 
        inner join tbt_receive_details d on t.id=d.receive_id 
        inner join tbt_ledgers l on d.ledger_id=l.id 
        inner join tbt_parts p on l.part_id=p.id 
        left join (select c.ledger_id,count(c.ledger_id) ctn from tbt_cartons c group by c.ledger_id) tc on d.ledger_id=tc.ledger_id 
        where t.receive_no like 'TI%' and (d.plan_ctn - (case when tc.ctn is null then 0 else tc.ctn end)) > 0
        order by p.no,g.batch_id,t.receive_no"""
        mycursor.execute(sql)
        rnd_x = 1
        for r in mycursor.fetchall():
            receive_body_id = str(r[0]).strip()
            batch_id = str(r[1]).strip()
            receive_no = str(r[2]).strip()
            part_no = str(r[3]).strip()
            plan_ctn = str(r[4]).strip()
            ledger_id = str(r[6]).strip()
            
            
            sql_ora = f"""SELECT '{receive_body_id}' rec_id,e.RECEIVINGDTE,t.RECEIVINGKEY,c.PARTNO,c.RVMANAGINGNO,c.LOTNO,c.RUNNINGNO,c.CASEID dieno,c.CASENO division,c.STOCKQUANTITY,t.OLDERKEY,c.SHELVE,c.PALLETKEY FROM TXP_RECTRANSBODY t  INNER JOIN TXP_RECTRANSENT e ON t.RECEIVINGKEY = e.RECEIVINGKEY  INNER JOIN TXP_CARTONDETAILS c ON t.RECEIVINGKEY = c.INVOICENO AND t.PARTNO = c.PARTNO AND t.RVMANAGINGNO=c.RVMANAGINGNO WHERE c.PARTNO='{part_no}' AND e.GEDI_FILE='{batch_id}' AND c.IS_CHECK=0 ORDER BY c.RUNNINGNO FETCH FIRST {plan_ctn} ROWS ONLY"""
            print(f"{rnd_x} :=> {receive_no} part: {part_no} batch id: {batch_id} ctn: {plan_ctn}") 
            rvm_no = None
            obj = Ora.execute(sql_ora)
            runx = 1
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
                sql_carton = f"""insert into tbt_cartons(id, ledger_id, lot_no, serial_no, die_no, revision_no, qty, is_active, created_at, updated_at)values('{carton_id}', '{ledger_id}', '{lotno}', '{serial_no}', '{die_no}', '{division_no}', '{std_pack}', true, current_timestamp, current_timestamp)"""
                if mycursor.fetchone() is None:
                    #### check stock
                    mycursor.execute(f"select id from tbt_stocks where ledger_id='{ledger_id}'")
                    if mycursor.fetchone() is None:
                        stock_id = generate(size=36)
                        mycursor.execute(f"""insert into tbt_stocks(id, ledger_id, per_qty, ctn, is_active, created_at, updated_at)values('{stock_id}', '{ledger_id}', {std_pack}, 0, true, current_timestamp, current_timestamp)""")
                    
                    
                    ### get stock
                    stock_check = Ora.execute(f"SELECT count(*) FROM  TXP_CARTONDETAILS WHERE PARTNO='{part_no}' AND SHELVE NOT IN ('S-PLOUT')")
                    ctn_stock = stock_check.fetchone()[0]
                    sql_update_stock = f"update tbt_stocks set per_qty='{std_pack}',ctn={ctn_stock},updated_at=current_timestamp where ledger_id='{ledger_id}'"
                    if int(std_pack) > 0:   
                        sql_update_stock = f"update tbt_stocks set ctn={ctn_stock},updated_at=current_timestamp where ledger_id='{ledger_id}'"
                    
                    mycursor.execute(sql_update_stock) 
                    
                else:
                    sql_carton = f"update tbt_cartons set ledger_id='{ledger_id}',updated_at=current_timestamp where serial_no='{serial_no}'"
                    
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
                Ora.execute(f"UPDATE TXP_CARTONDETAILS SET IS_CHECK=1 WHERE RUNNINGNO='{serial_no}'")
                print(f"{runx} ==> RVM NO: {rvm_no} SERIAL NO: {serial_no}")
                runx += 1
                
            #### update rvm no
            if rvm_no:
                log(name='CARTON', subject="SYNC RECEIVE", status="Success", message=f"Sync {receive_no} PART: {part_no} RVM NO: {rvm_no}")    
                sql_update_receive = f"update tbt_receive_details set managing_no='{rvm_no}',updated_at=current_timestamp where id='{receive_body_id}'"
                # print(sql_update_receive)
                mycursor.execute(sql_update_receive)
                print(f"end {rnd_x} :=> {receive_no} update part: {part_no} rvm: {rvm_no}\n") 
                
            ### commit data
            Conn.commit()
            mydb.commit()
            rnd_x += 1 # increment
        
    except Exception as ex:
        print(ex)
        pass
    
    mydb.close()
    
if __name__ == '__main__':
    get_receive()
    # main()
    # update_master_location()
    Conn.close()
    pool.release(Conn)
    pool.close()
    sys.exit(0)