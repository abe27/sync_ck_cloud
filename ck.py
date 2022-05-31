from datetime import datetime
import shutil
import sys
import os
import time
import psycopg2 as pgsql
import cx_Oracle
from nanoid import generate
from sqlalchemy import true
from spllibs import Yazaki, SplApi, SplSharePoint, LogActivity as log
from os.path import join, dirname
from dotenv import load_dotenv
# dotenv_path = join(dirname(__file__), '.env')
# load_dotenv(dotenv_path)
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

### Initail Data
yk = Yazaki(SERVICE_TYPE, YAZAKI_HOST, YAZAKI_USER, YAZAKI_PASSWORD)
spl = SplApi(SPL_API_HOST, SPL_API_USERNAME, SPL_API_PASSWORD)
share_file = SplSharePoint(SHAREPOINT_SITE_URL, SHAREPOINT_SITE_NAME, SHAREPOINT_USERNAME, SHAREPOINT_PASSWORD)

pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Oracur = Oracon.cursor()

def main():
    msg = f"Starting Sync CK on {YAZAKI_HOST}"
    log(subject="START", status='Active',message=msg)
    ### login
    session = yk.login()

    if session != False:
        ### get link gedi
        link = yk.get_link(session)
        i = 0
        while i < len(link):
            x = link[i]
            # ### download gedi file
            yk.download_gedi_files(session, x)
            
            print(f"download gedi file => {x.batchfile}")   
            i += 1
            
        ### logout
        yk.logout(session)
        
    ### Stop service
    log(subject="STOP", status='Active',message=f"SERVICE IS EXITED")
    
    
    ### Upload gedi to SPL Server
    batch_running = 1
    root_pathname = "EXPORT"
    # print(os.path.exists(root_pathname))
    if os.path.exists(root_pathname):
        log(name="SPL", subject="START", status='Active',message=f"Start SPL Service")
        spl_token = spl.login()
        if spl_token:
            ### Check Folder
            root_path = os.listdir(root_pathname)
            for x in root_path:
                xpath = os.listdir(f'{root_pathname}/{x}')
                for p in xpath:
                    pname = os.path.join(root_pathname, x, p)
                    for name in os.listdir(pname):
                        batchId = "{:08}".format(batch_running)
                        gedi_name = name
                        if name[:1] != "O":
                            batchId = str(name[:8]).replace('.', '')
                            gedi_name = name[8:]
                            
                        filepath = os.path.join(root_pathname, x, p, name)
                        print(f"Date: {p}")
                        print(f"WHS: CK-2")
                        print(f"Batch ID: {batchId}")
                        print(f"File Name: {gedi_name}")
                        print(f"TYPE: {x[:1]}")
                        print(f"Path: {filepath}")
                        ### upload file to SPL Server
                        is_upload = spl.upload("CK-2", x[:1], batchId, filepath, gedi_name, spl_token)
                        # is_upload = True
                        # ### Upload file to SPL Share Point
                        # share_file.upload(filepath, gedi_name, f'GEDI/{x}/{p}')
                        print(f"STATUS: {is_upload}")
                        batch_running += 1
                        print('-------------------------------------------\n')
                        time.sleep(1.5)
            
            is_success = spl.logout(spl_token)
            print(f'logout is {is_success}')

        log(name='SPL', subject="STOP", status='Active',message=f"Stop SPL Service")
        
    ### Delete EXPORT Folder
    if os.path.exists(root_pathname):
        shutil.rmtree(root_pathname)
        # if os.path.exists("BACKUP") is False:os.makedirs("BACKUP")
        # shutil.move(root_pathname, "BACKUP") 
        log(name='SPL', subject="DELETE", status='Active',message=f"Delete EXPORT Folder")
    
def download():
    ### Initail Mysql Server
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    token = spl.login()
    try:
        ### start get link download
        obj = spl.get_link(token)
        if len(obj) <= 0: return
        mycursor = mydb.cursor()
        
        i = 0
        while i < len(obj):
            r = obj[i]
            filename = spl.get_file(str(r['file_name']).replace('TXT', 'BIN'), r['file_path'])
            if filename:
                ### Order Plan
                if r['file_type'] == 'O':
                    head = spl.header_orderplan(filename)
                    data = []
                    f = open(filename, 'r')
                    for doc in f:
                        b = spl.read_orderplan(head, doc)
                        data.append(b)
                    f.close()
                    ### remove temp files after load data.
                    os.remove(filename)
                    ### Update status
                    spl.update_status(token, str(r['id']), 1)
                    sql = "INSERT INTO tbt_order_plans(id, file_gedi_id, vendor, cd, unit, whs, tagrp, factory, sortg1, sortg2, sortg3, plantype, pono, biac, shiptype, etdtap, partno, partname, pc, commercial, sampleflg, orderorgi, orderround, firmflg, shippedflg, shippedqty, ordermonth, balqty, bidrfl, deleteflg, ordertype, reasoncd, upddte, updtime, carriercode, bioabt, bicomd, bistdp, binewt, bigrwt, bishpc, biivpx, bisafn, biwidt, bihigh, bileng, lotno, is_active, created_at, updated_at)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true, %s, %s)"
                    for a in data:
                        # print(a)
                        id = generate(size=36)
                        update_date = f"{(a['upddte']).strftime('%Y-%m-%d')} {(a['updtime']).strftime('%H:%M:%S')}"
                        val = (id, r['id'], a['vendor'], a['cd'], a['unit'], a['whs'], a['tagrp'], a['factory'], a['sortg1'], a['sortg2'], a['sortg3'], a['plantype'], a['pono'], a['biac'], a['shiptype'], (a['etdtap']).strftime('%Y-%m-%d %H:%M:%S'), a['partno'], a['partname'], a['pc'], a['commercial'], a['sampleflg'], a['orderorgi'], a['orderround'], a['firmflg'], a['shippedflg'], a['shippedqty'], (a['ordermonth']).strftime('%Y-%m-%d %H:%M:%S'), a['balqty'], a['bidrfl'], a['deleteflg'], a['ordertype'], a['reasoncd'], (a['upddte']).strftime('%Y-%m-%d %H:%M:%S'), (a['updtime']).strftime('%Y-%m-%d %H:%M:%S'), a['carriercode'], a['bioabt'], a['bicomd'], a['bistdp'], a['binewt'], a['bigrwt'], a['bishpc'], a['biivpx'], a['bisafn'], a['biwidt'], a['bihigh'], a['bileng'],a['lotno'], update_date, update_date)
                        mycursor.execute(sql, val)
                        
                    ### Commit MySQL
                    mydb.commit()
                    
                    ### Log
                    log(name='SPL', subject="INSERT", status="Success", message=f"Insert Data Order Plan({len(data)})")
                
                #### For Receive
                elif r['file_type'] == 'R':
                    receive_no = []
                    head = spl.header_receive(filename)
                    ### GET Master
                    f = open(filename, 'r')
                    seq = 1
                    for doc in f:
                        h = doc
                        etd = datetime.strptime(str(h)[16:26], '%d/%m/%Y')
                        whs_id = r['whs_id']
                        mycursor.execute(f"select id from tbt_factory_types where name='{head['factory']}'")
                        myresult = mycursor.fetchone()
                        factory_id = myresult[0]
                        #### create receive header
                        if ((str(h)[4:16]) in receive_no) is False:receive_no.append(str(h)[4:16])
                        receive_id = generate(size=36)
                        
                        ### check duplicate
                        mycursor.execute(f"select id from tbt_receives where receive_no='{str(h)[4:16]}'")
                        receive_data = mycursor.fetchone()
                        if receive_data:
                            receive_id = receive_data[0]
                        else:
                            mycursor.execute(f"""insert into tbt_receives(id, whs_id, file_gedi_id, factory_type_id, receive_date, receive_no, receive_sync,is_active, created_at, updated_at)values('{receive_id}', '{whs_id}', '{r['id']}', '{factory_id}', '{etd}', '{str(h)[4:16]}', true, true, current_timestamp, current_timestamp)""")
                            
                        ### check body
                        part_id = None
                        b = spl.read_receive(head, doc)
                        partname = str(b['partname']).replace("'", "''")
                        ### Log
                        log(name='SPL', subject="INSERT", status="Success", message=f"Insert Data Receive {(str(h)[4:16])}({b['partno']})")
                        
                        
                        #### get master
                        ### get unit
                        mycursor.execute(f"select id from tbt_units where name='{b['unit']}'")
                        fetch_units = mycursor.fetchone()[0]
                        
                        ### get tagrp_id
                        mycursor.execute(f"select id from tbt_tagrps where name='{b['tagrp']}'")
                        tagrp_id = mycursor.fetchone()[0]
                        
                        #### check master part
                        mycursor.execute(f"select id from tbt_parts where no='{b['partno']}'")
                        fetch_parts = mycursor.fetchone()
                        if fetch_parts:
                            part_id = fetch_parts[0]
                            
                        else:
                            part_id = generate(size=36)
                            mycursor.execute(f"""insert into tbt_parts(id, no, name, is_active, created_at, updated_at)values('{part_id}', '{b['partno']}', '{partname}', true, current_timestamp, current_timestamp)""")
                        
                        mycursor.execute(f"select id from tbt_ledgers where part_id='{part_id}' and factory_id='{factory_id}' and whs_id='{whs_id}'")
                        ledgers = mycursor.fetchone()
                        if ledgers is None:
                            ### insert ledger
                            ledger_id = generate(size=36)
                            mycursor.execute(f"""insert into tbt_ledgers(id, tagrp_id, factory_id, whs_id, part_id, net_weight, gross_weight, unit_id, is_active, created_at, updated_at)
                                    values('{ledger_id}', '{tagrp_id}', '{factory_id}', '{whs_id}', '{part_id}', '{b['aenewt']}', '{b['aegrwt']}', '{fetch_units}', true, current_timestamp, current_timestamp)""")
                        else:
                            ledger_id = ledgers[0]
                            
                        plan_qty = b['plnqty']
                        plan_ctn = b['plnctn']
                        
                        ### insert receive body
                        mycursor.execute(f"select id from tbt_receive_details where receive_id='{receive_id}' and ledger_id='{ledger_id}'")
                        rec_body = mycursor.fetchone()
                        receive_body_id = generate(size=36)
                        sql_body = f"""insert into tbt_receive_details(id, receive_id, ledger_id, seq, managing_no, plan_qty, plan_ctn, is_active, created_at, updated_at)values('{receive_body_id}', '{receive_id}', '{ledger_id}', {seq}, '', {plan_qty}, {plan_ctn}, true, current_timestamp, current_timestamp)"""
                        if rec_body:
                            receive_body_id = rec_body[0]
                            sql_body = f"update tbt_receive_details set plan_qty='{plan_qty}', plan_ctn='{plan_ctn}',updated_at=current_timestamp where id='{receive_body_id}'"
                            
                        mycursor.execute(sql_body)
                        if seq == 6:
                            print(f"CHECK")
                            
                        print(f"Sync {str(h)[4:16]} Data :=> {seq} Part: {part_id}")
                        seq += 1
                        
                    f.close()
                    ### after insert data receive set update status receive_sync=True
                    for no in receive_no:
                        mycursor.execute(f"update tbt_receives set receive_sync=false where receive_no='{no}'")
                        ### Log
                        log(name='SPL', subject="UPDATE", status="Success", message=f"Update Status Sync Receive {no} Set False")
                        
                    mydb.commit()
                    ### remove temp files after load data.
                    os.remove(filename)
                    ### Update status
                    spl.update_status(token, str(r['id']), 1)
                    
            time.sleep(1.5)
            i += 1
            
    except Exception as ex:
        log(name='SPL', subject="INSERT", status="Error", message=str(ex))
        mydb.rollback()
        pass
    
    ### disconnect db
    mydb.close()
    if spl.logout(token):
        print(f'end service')
        
def get_receive():
    log(name='SPL', subject="START SYNC RECEIVE", status="Success", message=f"Get Receive Start Success")
    try:
        # Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
        #  Oracur = Oracon.cursor()
        token = spl.login()
        data = spl.get_receive(token)
        obj = data['data']
        receive_array = []
        for i in obj:
            receive_id = str(i['id'])
            body = spl.get_receive_body(token, i['id'], 1)
            sum_pln = 0
            x = 0
            while x < len(body):
                r = body[x]
                head = r['receive']
                batch_id = generate(size=7)
                try:
                    batch_id = r['receive']['file_gedi']['batch_id']
                except Exception as ex:
                    pass
                
                receive_no = head['receive_no']
                ### append to receive key
                if len(receive_array) == 0:
                    receive_array.append(receive_no)
                    
                if (receive_no in receive_array) is False:
                    receive_array.append(receive_no)
                
                ### check receive date
                receive_date = datetime.strptime(head['receive_date'], '%Y-%m-%d')
                factory_type = head['factory_type']['name']
                cd = '20'
                if factory_type == "AW": cd = "10"
                part = r['ledger']['part']['no']
                part_name = str(r['ledger']['part']['name']).replace("'","''")
                unit = r['ledger']['unit']['name']
                whs = r['ledger']['whs']['name']
                ### get part type
                part_type = "PART"
                sub_part = part[:2]
                if sub_part == "18":part_type = "WIRE"
                elif sub_part == "71":part_type = "PLATE"
                ### check part on master
                part_sql = Oracur.execute(f"select partno from txp_part where partno='{part}'")
                part_upd = "INSERT"
                sql_part_insert = f"""insert into txp_part(tagrp,partno,partname,carmaker,CD,TYPE,VENDORCD,UNIT ,upddte,sysdte)values('C','{part}','{part_name}','E', '{cd}', '{part_type}', '{factory_type}', '{unit}',sysdate,sysdate)"""
                if part_sql.fetchone():
                    part_upd = "UPDATE"
                    sql_part_insert = f"""update txp_part set  partname='{part_name}',upddte=sysdate where partno='{part}'"""
                Oracur.execute(sql_part_insert)
                
                ### check part on ledger
                print(r['plan_qty'])
                print(r['plan_ctn'])
                outer_qty = float(str(r['plan_qty']))/float(str(r['plan_ctn']))
                part_ledger_sql = Oracur.execute(f"select partno from TXP_LEDGER where partno='{part}'")
                ledger_sql = f"""INSERT INTO TXP_LEDGER(PARTNO,TAGRP,MINIMUM,MAXIMUM,WHS,PICSHELFBIN,STKSHELFBIN,OVSSHELFBIN,OUTERPCS,UPDDTE, SYSDTE)VALUES('{part}', 'C',0,0,'{factory_type}','PNON', 'SNON','ONON'0, sysdate, sysdate)"""
                if part_ledger_sql.fetchone():
                    ledger_sql = f"""UPDATE TXP_LEDGER SET RECORDMAX=1,LASTRECDTE=sysdate,LASTISSDTE=sysdate WHERE PARTNO='{part}'"""
                
                Oracur.execute(ledger_sql)    
                print(f"{part_upd} PART: {part} TYPE: {part_type} OUTER: {outer_qty}")
                
                ### get manno.
                rvm_no = (Oracur.execute(f"SELECT 'BD'|| SUBSTR(TO_CHAR(sysdate,'yyMMdd'), 2, 6)  || replace(to_char((SELECT count(*) FROM TXP_RECTRANSBODY WHERE TO_CHAR(SYSDTE, 'YYYYMMDD') = TO_CHAR(sysdate, 'YYYYMMDD')) + 1,'000099'),' ','') as genrunno  from dual")).fetchone()[0]
                
                ### delete data when duplicate
                Oracur.execute(f"DELETE TXP_RECTRANSBODY where RECEIVINGKEY='{receive_no}' AND PARTNO='{part}' AND RECCTN=0")
                ### receive body
                receive_body = Oracur.execute(f"""SELECT PARTNO from TXP_RECTRANSBODY WHERE RECEIVINGKEY='{receive_no}' AND PARTNO='{part}'""")
                sql_receive_body = f"""INSERT INTO TXP_RECTRANSBODY
                                    (RECEIVINGKEY, RECEIVINGSEQ, PARTNO, PLNQTY, PLNCTN,RECQTY,RECCTN,TAGRP, UNIT, CD, WHS, DESCRI, RVMANAGINGNO,UPDDTE, SYSDTE, CREATEDBY,MODIFIEDBY,OLDERKEY)
                                    VALUES('{receive_no}', '{(x + 1)}', '{part}', {r['plan_qty']}, {r['plan_ctn']},0,0,'C', '{unit}','{cd}' , '{whs}','{part_name}', '{rvm_no}',sysdate, sysdate, 'SKTSYS', 'SKTSYS', '{receive_no}')"""
                if receive_body.fetchone():
                    sql_receive_body = f"""UPDATE TXP_RECTRANSBODY SET PLNQTY='{r['plan_qty']}', PLNCTN={r['plan_ctn']} WHERE RECEIVINGKEY='{receive_no}'"""
                
                Oracur.execute(sql_receive_body)
                sum_pln += float(str(r['plan_ctn']))
                x += 1
                ### create receive ent
                receive_ent = Oracur.execute(f"SELECT RECEIVINGKEY from TXP_RECTRANSENT where RECEIVINGKEY='{receive_no}'")
                sql_rec_ent = f"""INSERT INTO TXP_RECTRANSENT(RECEIVINGKEY, RECEIVINGMAX, RECEIVINGDTE, VENDOR, RECSTATUS, RECISSTYPE, RECPLNCTN,RECENDCTN, UPDDTE, SYSDTE, GEDI_FILE)
                VALUES('{receive_no}', {len(body)}, to_date('{str(receive_date)[:10]}', 'YYYY-MM-DD'), '{factory_type}', 0, '01', 0,0, current_timestamp, current_timestamp, '{batch_id}')"""
                if receive_ent.fetchone():
                    sql_rec_ent = f"""UPDATE TXP_RECTRANSENT SET RECEIVINGMAX='{len(body)}',RECPLNCTN={sum_pln} WHERE RECEIVINGKEY='{receive_no}'"""
                
                Oracur.execute(sql_rec_ent)
                
            #### update receive ent status
            response = spl.update_receive_ent(token, receive_id, is_sync=1)
            if response is False:
                return
            
        spl.logout(token)
        Oracon.commit()
            
        ### notifications
        if len(receive_array) > 0:
            x = 0
            while x < len(receive_array):
                r = receive_array[x]
                fac = "AW"
                if r[:2] == "TI":fac = "INJ"
                recbody = Oracur.execute(f"SELECT sum(PLNCTN),count(PARTNO) FROM TXP_RECTRANSBODY WHERE RECEIVINGKEY='{r}'")
                p = recbody.fetchone()
                d = datetime.now()
                if p != None:
                    _ctn = f"{int(str(p[0])):,}"
                    msg = f"""{fac}\nเลขที่: {r}\nจำนวน: {p[1]} กล่อง: {_ctn}\nวดป.: {d.strftime('%Y-%m-%d %H:%M:%S')}"""
                    spl.line_notification(msg)
                    log(name='SPL', subject="SYNC RECEIVE", status="Success", message=f"Sync Receive({r})")
                x += 1
        
        #  Oracon.close()
        
        
    except Exception as ex:
        log(name='SPL', subject="SYNC RECEIVE", status="Error", message=str(ex))
        pass
    
def merge_receive():
    try:
        # Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
        #  Oracur = Oracon.cursor()
        ### GEDI BATCHID
        obj = Oracur.execute(f"SELECT GEDI_FILE,ctn  FROM (SELECT GEDI_FILE,count(GEDI_FILE) ctn FROM TXP_RECTRANSENT WHERE VENDOR='INJ' AND RECEIVINGKEY LIKE 'TI%' GROUP BY GEDI_FILE ORDER BY GEDI_FILE) WHERE CTN > 1")
        for r in obj.fetchall():
            #### get ent data
            receive_no = []
            receive_date = None
            key_no = None
            
            receive_list = []
            ent = Oracur.execute(f"SELECT RECEIVINGKEY, to_char(RECEIVINGDTE, 'YYYYMMDD')  FROM TXP_RECTRANSENT WHERE GEDI_FILE='{r[0]}' ORDER BY RECEIVINGKEY")
            for k in ent.fetchall():
                receive_date = k[1]
                if (k[0] in receive_list) is False:
                    receive_list.append(k[0])
                    receive_no.append(str(k[0])[10:])
            
            #### new receive key    
            sql_key = f"SELECT TO_CHAR(count(*) + 1, '09')  FROM TXP_RECTRANSENT t WHERE t.RECEIVINGKEY LIKE 'SI{datetime.now().strftime('%y%m%d')}%'"
            key = Oracur.execute(sql_key)
            key_no = (f"SI{datetime.now().strftime('%y%m%d')}{(key.fetchone())[0]}").replace(" ", "")
            receive_key = ",".join(receive_no)
            sql = f"""SELECT '{key_no}' RECEIVINGKEY,0 SEQ, PARTNO,sum(PLNQTY) PLNQTY,sum(PLNCTN) plnctn,0 RECQTY,0 RECCTN,TAGRP, UNIT, CD, WHS, DESCRI, '' RVMNO,sysdate UPDDTE, sysdate SYSDTE, 'SKTSYS' CREATEDBY,'SKTSYS' MODIFIEDBY,'{receive_key}' OLDERKEY  FROM TXP_RECTRANSBODY WHERE RECEIVINGKEY IN ({str(receive_list).replace('[', '').replace(']', '')}) GROUP BY PARTNO,TAGRP, UNIT, CD, WHS, DESCRI ORDER BY PARTNO"""
            # print(sql)
            body = Oracur.execute(sql)
            seq = 1
            ctn = 0
            for b in body.fetchall():
                ### get manno.
                rvm_no = (Oracur.execute(f"SELECT 'BD'|| SUBSTR(TO_CHAR(sysdate,'yyMMdd'), 2, 6)  || replace(to_char((SELECT count(*) FROM TXP_RECTRANSBODY WHERE TO_CHAR(SYSDTE, 'YYYYMMDD') = TO_CHAR(sysdate, 'YYYYMMDD')) + 1,'000099'),' ','') as genrunno  from dual")).fetchone()[0]
                ql_receive_body = f"""INSERT INTO TXP_RECTRANSBODY(RECEIVINGKEY, RECEIVINGSEQ, PARTNO, PLNQTY, PLNCTN,RECQTY,RECCTN,TAGRP, UNIT, CD, WHS, DESCRI, RVMANAGINGNO,UPDDTE, SYSDTE, CREATEDBY,MODIFIEDBY,OLDERKEY)VALUES('{key_no}', '{seq}', '{b[2]}', {b[3]}, {b[4]},0,0,'C', '{b[8]}','{b[9]}' , '{b[10]}','{str(b[11]).replace("'", "''")}', '{rvm_no}',sysdate, sysdate, 'SKTSYS', 'SKTSYS', '{receive_key}')"""
                Oracur.execute(ql_receive_body)
                ctn += float(str(b[4]))
                seq += 1
            
            sql_rec_ent = f"""INSERT INTO TXP_RECTRANSENT(RECEIVINGKEY, RECEIVINGMAX, RECEIVINGDTE, VENDOR, RECSTATUS, RECISSTYPE, RECPLNCTN,RECENDCTN, UPDDTE, SYSDTE, GEDI_FILE)
                VALUES('{key_no}', {seq}, to_date('{str(receive_date)}', 'YYYYMMDD'), 'INJ', 0, '01', {ctn},0, current_timestamp, current_timestamp, '{r[0]}')"""
            
            Oracur.execute(sql_rec_ent)   
            Oracur.execute(f"""INSERT INTO TMP_RECTRANSENT SELECT RECEIVINGKEY, RECEIVINGMAX, RECEIVINGDTE, RECSTATUS, RECPLNCTN, RECENDCTN, UPDDTE, '{key_no}' MERGEID, 0 SYNC FROM TXP_RECTRANSENT WHERE RECEIVINGKEY IN ({str(receive_list).replace('[', '').replace(']', '')})""")
            Oracur.execute(f"""INSERT INTO TMP_RECTRANSBODY SELECT RECEIVINGKEY, RECEIVINGSEQ, PARTNO, PLNQTY, RECQTY, PLNCTN, RECCTN, UNIT, RVMANAGINGNO, UPDDTE, '{key_no}' MERGEID, 0 SYNC FROM TXP_RECTRANSBODY WHERE RECEIVINGKEY IN ({str(receive_list).replace('[', '').replace(']', '')})""")
            ### after insert delete ent
            Oracur.execute(f"DELETE TXP_RECTRANSENT WHERE RECEIVINGKEY IN ({str(receive_list).replace('[', '').replace(']', '')})")
            Oracur.execute(f"DELETE TXP_RECTRANSBODY WHERE RECEIVINGKEY IN ({str(receive_list).replace('[', '').replace(']', '')})")
            
            d = datetime.now()
            _ctn = f"{ctn:,}"
            msg = f"""รวมรอบ INJ\nรอบ: {key_no}\nจำนวน: {seq} กล่อง: {_ctn}\nรอบที่รวม: {receive_key}\nวดป.: {d.strftime('%Y-%m-%d %H:%M:%S')}"""
            spl.line_notification(msg)
            log(name='SPL', subject="MRGE RECEIVE", status="Success", message=f"Merge Receive({key_no}) with {receive_key}")
            
        Oracon.commit()
        #  Oracon.close()
    except Exception as ex:
        log(name='SPL', subject="MERGE", status="Error", message=str(ex))
        pass
    
def update_receive_ctn():
    # Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
    #  Oracur = Oracon.cursor()
    sql = f"""SELECT t.RECEIVINGKEY,count(t.RECEIVINGKEY) seq, e.RECPLNCTN  ctn,CASE WHEN cc.rec_ctn IS NULL THEN 0 ELSE cc.rec_ctn END rec_ctn  FROM TXP_RECTRANSBODY t 
                INNER JOIN TXP_RECTRANSENT e ON t.RECEIVINGKEY = e.RECEIVINGKEY
                LEFT JOIN (
                    SELECT c.invoiceno,count(*) rec_ctn FROM TXP_CARTONDETAILS c GROUP BY c.invoiceno
                ) cc ON e.RECEIVINGKEY = cc.invoiceno
                WHERE TO_CHAR(e.RECEIVINGDTE, 'YYYYMMDD') = TO_CHAR(sysdate - 0, 'YYYYMMDD') 
                GROUP BY t.RECEIVINGKEY,e.RECPLNCTN,cc.rec_ctn"""
            
    rec = Oracur.execute(sql)
    data = rec.fetchall()
    for i in data:
        Oracur.execute(f"UPDATE TXP_RECTRANSENT SET RECEIVINGMAX='{i[1]}',RECPLNCTN={i[2]},RECENDCTN='{i[3]}' WHERE RECEIVINGKEY='{i[0]}'")
    
    ### commit the transaction
    Oracon.commit()
    # Oracur.close()
    #  Oracon.close()
    
def orderplans():
    token = spl.login()
    try:
        ### (f"start sync order plans")
        # Oracon = cx_Oracle.connect(user=ORA_PASSWORD,password=ORA_USERNAME,dsn=ORA_DNS)
        #  Oracur = Oracon.cursor()
        data = spl.get_order_plan(token, 5000, 0, 1)
        obj = data['data']
        if len(obj) == 0:
            #  Oracon.close()
            return spl.logout(token)
        
        order_id = []
        rnd = 1
        for i in obj:
            part_type = "PART"
            part = i['partno']
            part_name = str(i['partname']).replace("'", "''")
            cd = i['cd']
            factory_type = i['factory']
            unit = i['unit']
            outer_qty = i['bistdp']
            sub_part = part[:2]
            if sub_part == "18":part_type = "WIRE"
            elif sub_part == "71":part_type = "PLATE"
            ### check part on master
            part_sql = Oracur.execute(f"select partno from txp_part where partno='{part}'")
            part_upd = "INSERT"
            sql_part_insert = f"""insert into txp_part(tagrp,partno,partname,carmaker,CD,TYPE,VENDORCD,UNIT ,upddte,sysdte)values('C','{part}','{part_name}','E', '{cd}', '{part_type}', '{factory_type}', '{unit}',sysdate,sysdate)"""
            if part_sql.fetchone():
                part_upd = "UPDATE"
                sql_part_insert = f"""update txp_part set  partname='{part_name}',upddte=sysdate where partno='{part}'"""
            Oracur.execute(sql_part_insert)
            
            ### check part on ledger
            part_ledger_sql = Oracur.execute(f"select partno from TXP_LEDGER where partno='{part}'")
            ledger_sql = f"""INSERT INTO TXP_LEDGER(PARTNO,TAGRP,MINIMUM,MAXIMUM,WHS,PICSHELFBIN,STKSHELFBIN,OVSSHELFBIN,OUTERPCS,UPDDTE, SYSDTE)VALUES('{part}', 'C',0,0,'{factory_type}','PNON', 'SNON','ONON',{outer_qty}, sysdate, sysdate)"""
            if part_ledger_sql.fetchone():
                ledger_sql = f"""UPDATE TXP_LEDGER SET OUTERPCS={outer_qty},RECORDMAX=1,LASTRECDTE=sysdate,LASTISSDTE=sysdate WHERE PARTNO='{part}'"""
            
            Oracur.execute(ledger_sql)
            ### create order plan
            factory =  i['factory']
            shiptype =  i['shiptype']
            affcode =  i['biac']
            pono =  i['pono']
            etdtap =  i['etdtap']
            partno =  i['partno']
            partname =  str(i['partname']).replace("'", "''")
            ordermonth =  i['ordermonth']
            orderorgi =  i['orderorgi']
            orderround =  i['orderround']
            balqty =  i['balqty']
            shippedflg =  i['shippedflg']
            shippedqty =  i['shippedqty']
            pc =  i['pc']
            commercial =  i['commercial']
            sampflg =  i['sampleflg']
            carriercode =  i['carriercode']
            ordertype =  i['ordertype']
            allocateqty =  i['allocateqty']
            bidrfl =  i['bidrfl']
            deleteflg =  i['deleteflg']
            reasoncd =  i['reasoncd']
            bioabt =  i['bioabt']
            firmflg =  i['firmflg']
            bicomd =  i['bicomd']
            bistdp =  i['bistdp']
            binewt =  i['binewt']
            bigrwt =  i['bigrwt']
            bishpc =  i['bishpc']
            biivpx =  i['biivpx']
            bisafn =  i['bisafn']
            bileng =  i['bileng']
            biwidt =  i['biwidt']
            bihigh =  i['bihigh']
            createdby =  'SKTSYS'
            modifiedby =  'SKTSYS'
            lotno =  i['lotno']
            orderid =  i['id']
            sql_order_plan = f"""INSERT INTO TXP_ORDERPLAN(FACTORY, SHIPTYPE, AFFCODE, PONO, ETDTAP, PARTNO, PARTNAME, ORDERMONTH, ORDERORGI, ORDERROUND, BALQTY, SHIPPEDFLG, SHIPPEDQTY, PC, COMMERCIAL, SAMPFLG, CARRIERCODE, ORDERTYPE, UPDDTE, ALLOCATEQTY, BIDRFL, DELETEFLG, REASONCD, BIOABT, FIRMFLG, BICOMD, BISTDP, BINEWT, BIGRWT, BISHPC, BIIVPX, BISAFN, BILENG, BIWIDT, BIHIGH, SYSDTE, CREATEDBY, MODIFIEDBY, LOTNO, ORDERID)VALUES('{factory}','{shiptype}','{affcode}','{pono}',to_date('{etdtap}', 'YYYY-MM-DD'),'{partno}','{partname}',to_date('{ordermonth}', 'YYYY-MM-DD'),'{orderorgi}','{orderround}','{balqty}','{shippedflg}','{shippedqty}','{pc}','{commercial}','{sampflg}','{carriercode}','{ordertype}',sysdate,'{allocateqty}','{bidrfl}','{deleteflg}','{reasoncd}','{bioabt}','{firmflg}','{bicomd}','{bistdp}','{binewt}','{bigrwt}','{bishpc}','{biivpx}','{bisafn}','{bileng}','{biwidt}','{bihigh}',sysdate,'{createdby}','{modifiedby}','{lotno}','{orderid}')"""
            Oracur.execute(sql_order_plan)
            print(f"{rnd} => {part_upd} LEDGER PART {part} {part_name}")
            order_id.append(orderid)
            rnd += 1
            
        Oracon.commit()
        
        ### after insert orderplan
        if len(order_id) > 0:
            mydb = pgsql.connect(
                host=DB_HOSTNAME,
                port=DB_PORT,
                user=DB_USERNAME,
                password=DB_PASSWORD,
                database=DB_NAME,
            )
            
            mycursor = mydb.cursor()
            mycursor.execute(f"update tbt_order_plans set is_sync=true where id in ({str(order_id).replace('[', '').replace(']', '')})")
            mydb.commit()
            mydb.close()
        
        d = datetime.now()
        _rnd = f"{(rnd - 1):,}"
        msg = f"""ซิงค์ข้อมูล OrderPlan\nจำนวน: {_rnd} รายการ\nวดป.: {d.strftime('%Y-%m-%d %H:%M:%S')}"""
        if int(rnd) > 0:
            spl.line_notification(msg)
        
        print(f"---------------------")   
        print(msg)
        log(name='ORDERPLAN', subject="ORDERPLAN", status="Success", message=str(msg))
        
    except Exception as ex:
        log(name='ORDERPLAN', subject="ORDERPLAN", status="Error", message=str(ex))
        pass
    
    #  Oracon.close()
    spl.logout(token)
    

def update_order_group():
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"""select vendor,bishpc,shiptype,pono,vendor||bioabt zone_name from tbt_order_plans where is_sync=false group by vendor,bishpc,shiptype,pono,bioabt  order by vendor,bishpc,shiptype,pono,bioabt""")
    for i in mycursor.fetchall():
        factory = str(i[0]).strip()
        bishpc = str(i[1]).strip()
        shiptype = str(i[2]).strip()
        pono = str(i[3]).strip()
        bioat = str(i[4]).strip()
        
        bioat_name = "CK-2"
        prefix_order = ""
        if pono[:1] == "#":
            bioat_name = "NESC"
            prefix_order = "#"
            
        elif pono[:1] == "@":
            bioat_name = "ICAM"
            prefix_order = "@"
        
        order_group = "ALL"
        if bishpc == "32W2":
            order_group = prefix_order+order_group
            order_list = ["NHW","HMW","LMW","WMW", " MW"]
            if (pono[len(pono) - 3:] in order_list):
                order_group = prefix_order+pono[len(pono) - 3:]
                
        elif bishpc == "32H7":
            order_group = prefix_order+order_group
            order_list = ["DMW","TMW","CMW", " MW"]
            if (pono[len(pono) - 3:] in order_list):
                order_group = prefix_order+pono[len(pono) - 3:]       
                
        elif bishpc == "32CJ":
            order_list = ["JQN","JTK"]
            if (pono[:3] in order_list):
                order_group = pono[:3]
                if pono[:1] == "#" or pono[:1] == "@":
                    order_group = pono[:4]  
                    
        elif bishpc == "32N1":
            order_list = ["NST","NTT","FBF"]
            if (pono[:3] in order_list):
                order_group = pono[:3]
                if pono[:1] == "#" or pono[:1] == "@":
                    order_group = pono[:4]
                    
        elif bishpc == "32AF":
            order_list = ["JTB","TIK"]
            if (pono[:3] in order_list):
                order_group = pono[:3]
                if pono[:1] == "#" or pono[:1] == "@":
                    order_group = pono[:4]
        
        elif bishpc == "32H2":
            order_list = ["TIJ","JQK"]
            if (pono[:3] in order_list):
                order_group = pono[:3]
                if pono[:1] == "#" or pono[:1] == "@":
                    order_group = pono[:4]
                    
        elif bishpc in ["32BF","32H0","32G0","32R8","32W6","32W7","32BG","32R1","32R4"]:
            order_group = pono
        
        elif bishpc[:3] == "32W":
            order_group = f"{prefix_order}{bishpc[2:]}"
            
        else:
            order_group = prefix_order+"ALL"
        
        sql_order_group = f"update tbt_order_plans set order_group='{str(order_group).strip()}',is_sync=true where vendor='{factory}' and bishpc='{bishpc}' and shiptype='{shiptype}' and pono='{pono}' and vendor||bioabt='{bioat}'"        
        print(f"SHIP: {shiptype} CUSTOMER: {bishpc} ORDER GROUP: {str(order_group).strip()} ORDERNO.: {pono} ZONE: {bioat_name}")
        mycursor.execute(sql_order_group)
        
    mydb.commit()
    mydb.close()
    
if __name__ == '__main__':
    main()
    time.sleep(0.1)
    download()
    time.sleep(0.1)
    get_receive()
    time.sleep(0.1)
    merge_receive()
    time.sleep(0.1)
    update_receive_ctn()
    time.sleep(0.1)
    update_order_group()
    # time.sleep(0.1)
    # orderplans()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)