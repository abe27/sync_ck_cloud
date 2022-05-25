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

pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Oracur = Oracon.cursor()

def update_die():
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    
    #  Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
    # Oracur = Oracon.cursor()
        
    try:
        log(name='MASTER', subject="Start", status="Success", message=f"Sync Shelve")    
        mycursor = mydb.cursor()
        mycursor.execute("""select t.id,t.die_no,t.serial_no  from tbt_cartons t where t.die_no='None' order by t.serial_no""")
        
        data = mycursor.fetchall()
        for i in data:
            carton_id = str(i[0])
            die_no = str(i[1])
            serial_no = str(i[2])
            ora_sql = f"""SELECT CASEID,CASENO  FROM TXP_CARTONDETAILS WHERE RUNNINGNO='{serial_no}'"""
            obj = Oracur.execute(ora_sql)
            for x in obj.fetchall():
                die_no = str(x[0])
                division_no = str(x[1]).replace('None', '')
                #### create carton on stock Cloud
                mycursor.execute(f"update tbt_cartons set die_no='{die_no}', division_no='{division_no}' where id='{carton_id}'")
                
        mydb.commit()
        log(name='MASTER', subject="END", status="Success", message=f"Sync Shelve")    
    except Exception as ex:
        log(name='MASTER', subject="UPLOAD SHELVE", status="Error", message=str(ex))
        Oracon.rollback()
        mydb.rollback()
        pass
    
    # Oracon.commit()
    #  Oracon.close()
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
    #  Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
    # Oracur = Oracon.cursor()
    
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
        log(name='MASTER', subject="UPLOAD RECEIVE", status="Error", message=str(ex))
        pass
    
    # Oracon.commit()
    #  Oracon.close()
    mydb.close()
    
def update_carton():
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    mycursor = mydb.cursor()
    #  Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
    # Oracur = Oracon.cursor()
    log(name='MASTER', subject="UPDATE STOCK", status="Success", message=f"Start Service")
    try:
        mycursor.execute(f"""select c.id,d.ledger_id,c.serial_no,d.managing_no,c.qty  from tbt_cartons c inner join tbt_receive_details d on c.receive_detail_id=d.id order by c.lot_no,c.serial_no""")
        # txt = "UPDATE"
        rnd = 1
        for i in mycursor.fetchall():
            id = str(i[0])
            ledger_id = str(i[1])
            serial_no = str(i[2])
            managing_no = str(i[3])
            sql = f"SELECT '{id}' carton_id,'{ledger_id}' ledger_id, PALLETKEY,CASEID,SHELVE,STOCKQUANTITY  FROM TXP_CARTONDETAILS WHERE RUNNINGNO='{serial_no}' AND RVMANAGINGNO='{managing_no}'"
            obj = Oracur.execute(sql)
            for x in obj.fetchall():
                # CARTON_ID = str(x[0]).replace("None", "")
                # LEDGER_ID = str(x[1]).replace("None", "")
                PALLETKEY = str(x[2]).replace("None", "-")
                CASEID = str(x[3]).replace("None", "")
                SHELVE = str(x[4]).replace("None", "-")
                STOCKQUANTITY = float(str(x[5]))
                
                ### get location_id
                mycursor.execute(f"select id from tbt_locations where name='{SHELVE}'")
                location_id = mycursor.fetchone()[0]
                
                ### update shelve
                mycursor.execute(f"select id from tbt_shelves where carton_id='{id}' and location_id='{location_id}'")
                shelve = mycursor.fetchone()
                txt = f"insert id: {id}"
                sql_shelve = f"""insert into tbt_shelves(id, carton_id, location_id, pallet_no, is_printed, is_active, created_at, updated_at)values('{generate(size=36)}', '{id}', '{location_id}', '{PALLETKEY}', false, false, current_timestamp, current_timestamp)"""
                if shelve:
                    shelve_id = shelve[0]
                    sql_shelve = f"""update tbt_shelves set location_id='{location_id}', pallet_no='{PALLETKEY}',updated_at=current_timestamp where id='{shelve_id}'"""
                    txt = f"update id: {id}"
                
                mycursor.execute(sql_shelve)
                print(f"{rnd}. {txt}")
                ### update carton
                sql_update_carton = f"update tbt_cartons set qty='{STOCKQUANTITY}',updated_at=current_timestamp where id='{id}'"
                mycursor.execute(sql_update_carton)
                rnd += 1
                
            log(name='MASTER', subject="UPDATE STOCK", status="Success", message=f"Update Carton {serial_no}")
        
        # print(f"{txt} {str(x[0])}")
        mydb.commit()
        
    except Exception as ex:
        log(name='MASTER', subject="UPDATE STOCK", status="Error", message=str(ex))
        pass
    
    # Oracon.commit()
    #  Oracon.close()
    mydb.close()
    log(name='MASTER', subject="UPDATE STOCK", status="Success", message=f"End Service")
    
def update_stock():
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    mycursor = mydb.cursor()
    #  Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
    # Oracur = Oracon.cursor()
    log(name='MASTER', subject="UPDATE STOCK", status="Success", message=f"Start Service")
    try:
        sql_get_stock = f"SELECT PARTNO,sum(1) on_stock, RECEIVINGQUANTITY  FROM TXP_CARTONDETAILS GROUP BY PARTNO,RECEIVINGQUANTITY ORDER BY PARTNO"
        data = Oracur.execute(sql_get_stock)
        r = data.fetchall()
        rnd = len(r)
        runn = 1
        for i in r:
            # if runn > 170:
            #     print(f"check")
            part_no = str(i[0])
            on_stock = float(str(i[1]))
            stdpack = float(str(i[2]))
            ### get unit
            mycursor.execute(f"select id from tbt_whs where name='CK-2'")
            whs_id = mycursor.fetchone()[0]
            
            mycursor.execute(f"select id from tbt_parts where no='{part_no}'")
            part_id = mycursor.fetchone()
            if part_id:
                part_id = part_id[0]
                
            else:
                part_type_name = "PART"
                part_unit = "BOX"
                part_factory = "INJ"
                if str(part_no)[:2] == '71':
                    part_type_name = "PLATE"
                    
                elif str(part_no)[:2] == '18':
                    part_type_name = "WIRE"
                    part_unit = "COIL"
                    part_factory = "AW"
                
                ### get part type    
                mycursor.execute(f"select id from tbt_part_types where name='{part_type_name}'")
                part_type_id = mycursor.fetchone()[0]
                
                ### get factory
                mycursor.execute(f"select id from tbt_factory_types where name='{part_factory}'")
                factory_type_id = mycursor.fetchone()[0]
                
                ### get unit
                mycursor.execute(f"select id from tbt_units where name='{part_unit}'")
                unit_id = mycursor.fetchone()[0]
                
                ### get tagrp_id
                mycursor.execute(f"select id from tbt_tagrps where name='C'")
                tagrp_id = mycursor.fetchone()[0]
                
                part_id = generate(size=36)
                mycursor.execute(f"""insert into tbt_parts(id, no, name, is_active, created_at, updated_at)values('{part_id}', '{part_no}', '{part_no}', true, current_timestamp, current_timestamp)""")
                ledger_id = generate(size=36)
                mycursor.execute(f"""insert into tbt_ledgers(id, part_type_id, tagrp_id, factory_id, whs_id, part_id, net_weight, gross_weight, unit_id, is_active, created_at, updated_at)values('{ledger_id}', '{part_type_id}', '{tagrp_id}', '{factory_type_id}', '{whs_id}', '{part_id}', '0', '0', '{unit_id}', true, current_timestamp, current_timestamp)""")
                            
                
            mycursor.execute(f"select id from tbt_ledgers where part_id='{part_id}' and whs_id='{whs_id}'")
            ledger_id = mycursor.fetchone()[0]
            mycursor.execute(f"select id from tbt_stocks  where ledger_id='{ledger_id}'")
            
            txt = "UPDATE"
            sql_update_stock = f"update tbt_stocks set per_qty='{stdpack}',ctn='{on_stock}',updated_at=current_timestamp where ledger_id='{ledger_id}'"
            if mycursor.fetchone() is None:
                txt = "INSERT"
                sql_update_stock = f"insert into tbt_stocks(id,ledger_id,per_qty,ctn,is_active,created_at,updated_at)values('{generate(size=36)}','{ledger_id}','{stdpack}','{on_stock}',true,current_timestamp,current_timestamp)"

            # print(sql_update_stock)
            mycursor.execute(sql_update_stock)
            msg = f"{runn} ==> {int(((runn*100)/rnd))} % {txt} {part_no} SET {on_stock}"
            log(name='MASTER', subject="UPDATE STOCK", status="Success", message=msg)
            print(msg)
            runn += 1
        
        # print(f"{txt} {str(x[0])}")
        mydb.commit()
        
    except Exception as ex:
        log(name='MASTER', subject="UPDATE STOCK", status="Error", message=str(ex))
        mydb.rollback()
        pass
    
    # Oracon.commit()
    #  Oracon.close()
    mydb.close()
    log(name='MASTER', subject="UPDATE STOCK", status="Success", message=f"End Service")
    
if __name__ == '__main__':
    update_stock()
    time.sleep(0.1)
    update_master_location()
    time.sleep(0.1)
    update_die()
    time.sleep(0.1)
    update_carton()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)