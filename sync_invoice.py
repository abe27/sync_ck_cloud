import sys
import os
import cx_Oracle
import pandas as pd
import psycopg2 as pgsql
from datetime import datetime
from nanoid import generate
from dotenv import load_dotenv
load_dotenv()

DB_HOSTNAME = os.environ.get('DATABASE_URL')
DB_PORT = os.environ.get('DATABASE_PORT')
DB_NAME = os.environ.get('DATABASE_NAME')
DB_USERNAME = os.environ.get('DATABASE_USERNAME')
DB_PASSWORD = os.environ.get('DATABASE_PASSWORD')

ORA_DNS=f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME=os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD=os.environ.get('ORAC_DB_PASSWORD')

### Initail Data

pool = cx_Oracle.SessionPool(user=ORA_PASSWORD, password=ORA_USERNAME, dsn=ORA_DNS, min=2, max=100, increment=1, encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Oracur = Oracon.cursor()

### Initail PostgreSQL Server
pgdb = pgsql.connect(
    host=DB_HOSTNAME,
    port=DB_PORT,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    database=DB_NAME,
)
pg_cursor = pgdb.cursor()

def main():
    sql = f"""select 
        tft.factory_prefix||tc.prefix_code||to_char(td.etd_date, 'y') prefix_issuingkey,ti.running_seq,0 issuingstatus,td.etd_date etddte,tft.name factory,ta.aff_code affcode,tcc.cust_code bishpc,tcc.cust_name custname,td.commercial comercial,td.bioabt zoneid,ts.prefix_code shiptype,case when td.order_type = '-' then 'E' else td.order_type end combinv,td.pc pc,ti.zone_code zonecode,ti.ship_via note1,ti.privilege note2,td.id uuid,'SKTSYS' createdby,'SKTSYS' modifiedby,ti.ship_der containertype,0 issuingmax,ti.references_id,ti.id inv_id
    from tbt_orders td
    inner join tbt_consignees tc on td.consignee_id = tc.id 
    inner join tbt_factory_types tft on tc.factory_id = tft.id
    inner join tbt_affiliates ta on tc.aff_id = ta.id 
    inner join tbt_customers tcc on tc.customer_id=tcc.id
    inner join tbt_shippings ts on td.shipping_id = ts.id
    inner join tbt_invoices ti on ti.order_id = td.id
    where ti.is_sync_to_system=false
    order by td.etd_date,tc.prefix_code,ti.running_seq,ti.references_id"""
    pg_cursor.execute(sql)
    db = pg_cursor.fetchall()
    for i in db:
        prefix_issuingkey = str(i[0])
        running_seq = int(str(i[1]))
        issuingstatus = str(i[2])
        etddte = str(i[3])
        factory = str(i[4])
        affcode = str(i[5])
        bishpc = str(i[6])
        custname = str(i[7])
        comercial = str(i[8])
        zoneid = str(i[9])
        shiptype = str(i[10])
        combinv = str(i[11])
        pc = str(i[12])
        zonecode = str(i[13])
        note1 = str(i[14])
        note2 = str(i[15])
        uid = str(i[16])
        createdby = str(i[17])
        modifiedby = str(i[18])
        containertype = str(i[19])
        issuingmax = str(i[20])
        references_id = str(i[21]).strip()
        inv_id = str(i[22]).strip()
        
        invoiceno = f"{prefix_issuingkey}{'{:04d}'.format(running_seq)}{shiptype}"
        
        # print(txt)  
        # print(sql_insert_header)
        sql_inv_body = f"""select '{invoiceno}' issuingkey,ROW_NUMBER () OVER (order by top.sequence) seq,tod.pono,'C' tagrp,top.partno,top.bistdp stdpack,top.balqty orderqty,0 issueokqty,0 shorderqty,0 prepareqty,0 revisedqty,0 issuedqty,0 issuingstatus,top.biwidt bwide,top.bileng bleng,top.bihigh bhight,top.binewt neweight,top.bigrwt gtweight,'PART' parttype,top.partname,top.shiptype,top.etdtap,tod.id uuid,'SKTSYS' createdby,'SKTSYS' modifiedby,top.ordertype ordertype,top.lotno,'{invoiceno}' refinv
        from tbt_order_details tod 
        inner join tbt_order_plans top on tod.order_plan_id = top.id
        where tod.order_id='{uid}'"""
        pg_cursor.execute(sql_inv_body)
        part = pg_cursor.fetchall()
        for r in part:
            seq = int(str(r[1]).strip())
            pono = str(r[2]).strip()
            tagrp = str(r[3]).strip()
            partno = str(r[4]).strip()
            stdpack = float(str(r[5]).strip())
            orderqty = float(str(r[6]).strip())
            issueokqty = float(str(r[7]).strip())
            shorderqty = float(str(r[8]).strip())
            prepareqty = float(str(r[9]).strip())
            revisedqty = float(str(r[10]).strip())
            issuedqty = float(str(r[11]).strip())
            issuingstatus = str(r[12]).strip()
            bwide = float(str(r[13]).strip())
            bleng = float(str(r[14]).strip())
            bhight = float(str(r[15]).strip())
            neweight = float(str(r[16]).strip())
            gtweight = float(str(r[17]).strip())
            parttype = 'P'#str(r[18]).strip()
            partname = str(r[19]).strip()
            etdtap = str(r[21]).strip()
            uuid = str(r[22]).strip()
            ordertype = str(r[25]).strip()
            lotno = str(r[26]).strip()
            refinv = str(r[27]).strip()
            
            inv_body = Oracur.execute(f"SELECT ISSUINGKEY FROM TXP_ISSTRANSBODY WHERE ISSUINGKEY='{references_id}' AND PARTNO='{partno}' AND PONO='{pono}'")
            sql_inv_body = f"""INSERT INTO TXP_ISSTRANSBODY(issuingkey,issuingseq,pono,tagrp,partno,stdpack,orderqty,issueokqty,shorderqty,prepareqty,revisedqty,issuedqty,issuingstatus,bwide,bleng,bhight,neweight,gtweight,upddte,sysdte,parttype,partname,shiptype,edtdte,uuid,createdby,modifiedby,ordertype,lotno,refinv)
            VALUES('{references_id}','{seq}','{pono}','{tagrp}','{partno}','{stdpack}','{orderqty}','{issueokqty}','{shorderqty}','{prepareqty}','{revisedqty}','{issuedqty}','{issuingstatus}','{bwide}','{bleng}','{bhight}','{neweight}','{gtweight}',sysdate,sysdate,'{parttype}','{partname}','{shiptype}',to_date('{etdtap[:10]}', 'YYYY-MM-DD'),'{uuid}','{createdby}','{modifiedby}','{ordertype}','{lotno}','{refinv}')"""
            if inv_body.fetchone():
                sql_inv_body = f"""UPDATE TXP_ISSTRANSBODY SET stdpack='{stdpack}',orderqty='{orderqty}' WHERE ISSUINGKEY='{references_id}' AND PARTNO='{partno}' AND PONO='{pono}'"""
                
            Oracur.execute(sql_inv_body)
        
        issuingmax = len(part)
        inv = Oracur.execute(f"SELECT ISSUINGKEY FROM TXP_ISSTRANSENT WHERE ISSUINGKEY='{references_id}'")
        sql_insert_header = f"""INSERT INTO TXP_ISSTRANSENT(ISSUINGKEY, ETDDTE, FACTORY, AFFCODE, BISHPC, CUSTNAME, COMERCIAL, ZONEID, SHIPTYPE, COMBINV, PC,  ZONECODE, NOTE1, NOTE2, NOTE3, ISSUINGMAX, ISSUINGSTATUS,UPDDTE, SYSDTE, UUID, CREATEDBY, MODIFIEDBY,REFINVOICE)
        VALUES('{references_id}', to_date('{etddte[:10]}', 'YYYY-MM-DD'), '{factory}', '{affcode}', '{bishpc}', '{custname}', '{comercial}', '{zoneid}', '{shiptype}', '{combinv}', '{pc}', '{zonecode}', '{note1}', '{note2}', '{containertype}', '{issuingmax}', '0',sysdate, sysdate, '{uid}', '{createdby}', '{modifiedby}', '{invoiceno}')"""
        txt = "INSERT"
        if inv.fetchone():
            sql_insert_header = f"update TXP_ISSTRANSENT set ETDDTE=to_date('{etddte[:10]}', 'YYYY-MM-DD'), FACTORY='{factory}', AFFCODE='{affcode}', BISHPC='{bishpc}', CUSTNAME='{custname}', COMERCIAL='{comercial}', ZONEID='{zoneid}', SHIPTYPE='{shiptype}', COMBINV='{combinv}', PC='{pc}',  ZONECODE='{zonecode}', NOTE1='{note1}', NOTE2='{note2}', NOTE3='{containertype}', ISSUINGMAX='{issuingmax}' where ISSUINGKEY='{references_id}'"
            txt = "UPDATED"
        Oracur.execute(sql_insert_header)
        pg_cursor.execute(f"update tbt_orders set sync=true where id='{uid}'")
        pg_cursor.execute(f"update tbt_invoices set is_sync_to_system=true,updated_at=current_timestamp where id='{inv_id}'")
        sql_update_orderplan = f"UPDATE TXP_ORDERPLAN SET CURINV='{references_id}' WHERE FACTORY='{factory}' AND AFFCODE='{affcode}' AND  BISHPC='{bishpc}' AND BISAFN='{custname}' AND SHIPTYPE='{shiptype}' AND BIOABT='{zoneid}' AND TO_CHAR(ETDTAP, 'YYYY-MM-DD')='{etddte}'"
        Oracur.execute(sql_update_orderplan)
        print(f"{txt} ==> {uid}")
        
        ### sync Pallet
        sql_pallet = f"""select '{factory}' factory,'{references_id}' issuingkey,to_char(tip.pallet_no, '000') palletno,'{custname}' custname,case when tip.placing_id is null then 'C' else 'P' end  pltype,tip.pallet_total pltotal,case when tip.placing_id is null then top.biwidt else tpop.pallet_width end plwide,case when tip.placing_id is null then top.bileng else tpop.pallet_length end plleng,case when tip.placing_id is null then top.bihigh else tpop.pallet_height end plhight
        from tbt_invoice_pallets tip 
        left join tbt_placing_on_pallets tpop on tip.placing_id=tpop.id
        inner join tbt_invoice_pallet_details tipd on tip.id=tipd.invoice_pallet_id 
        inner join tbt_order_details tod on tipd.invoice_part_id=tod.id
        inner join tbt_order_plans top on tod.order_plan_id=top.id where tip.invoice_id='{inv_id}'
        order by tip.pallet_no"""
        pg_cursor.execute(sql_pallet)
        pldb = pg_cursor.fetchall()
        for i in pldb:
            factory = str(i[0]).strip()
            issuingkey = str(i[1]).strip()
            palletno = str(i[2]).strip()
            custname = str(i[3]).strip()
            pltype = str(i[4]).strip()
            pltotal = str(i[5]).strip()
            plwide = str(i[6]).strip()
            plleng = str(i[7]).strip()
            plhight = str(i[8]).strip()
            plnum = pltype + palletno
            pltype_name = pltype
            
            ### Check Duplicate
            sql_pl_duplicate = f"SELECT PALLETNO FROM TXP_ISSPALLET WHERE ISSUINGKEY='{issuingkey}' AND PALLETNO='{plnum}'"
            pl_ora = Oracur.execute(sql_pl_duplicate)
            sql_insert_pallet = f"""INSERT INTO TXP_ISSPALLET(FACTORY,ISSUINGKEY,PALLETNO,CUSTNAME,PLTYPE,PLOUTSTS,UPDDTE,SYSDTE,PLTOTAL,PLWIDE,PLLENG,PLHIGHT)VALUES('{factory}','{issuingkey}','{plnum}','{custname}','{pltype_name}',0,sysdate,sysdate,'{pltotal}',{plwide},{plleng},{plhight})"""
            if pl_ora.fetchone() != None:
                sql_insert_pallet = f"""UPDATE TXP_ISSPALLET SET UPDDTE=sysdate,PLTOTAL='{pltotal}',PLWIDE={plwide},PLLENG={plleng},PLHIGHT={plhight} WHERE ISSUINGKEY='{issuingkey}' AND PALLETNO='{plnum}'"""
            
            Oracur.execute(sql_insert_pallet)
            
        ### Sync Invoice Detail
        sql_fticket = f"""select '{issuingkey}' issuingkey,tod.pono pono,'C' tagrp,top.partno partno,tf.fticket_no fticketno,top.bistdp orderqty,0 issuedqty,case when tip.placing_id is null then 'C' else 'P' end  pltype,to_char(tip.pallet_no, '000') shipplno,top.unit unit,0 issuingstatus,tf.id uuid,'SKTSYS' createdby,'SKTSYS' modifedby,todd.order_group splorder
        from tbt_invoice_pallets tip 
        left join tbt_placing_on_pallets tpop on tip.placing_id=tpop.id
        inner join tbt_invoice_pallet_details tipd on tip.id=tipd.invoice_pallet_id 
        inner join tbt_order_details tod on tipd.invoice_part_id=tod.id
        inner join tbt_orders todd on tod.order_id=todd.id 
        inner join tbt_order_plans top on tod.order_plan_id=top.id 
        inner join tbt_ftickets tf on tipd.id=tf.invoice_pallet_detail_id
        where tip.invoice_id='{inv_id}'
        order by tf.fticket_no,tip.pallet_no"""
        
        pg_cursor.execute(sql_fticket)
        for p in pg_cursor.fetchall():
            issuingkey = str(p[0]).strip()
            pono = str(p[1]).strip()
            tagrp = str(p[2]).strip()
            partno = str(p[3]).strip()
            fticketno = str(p[4]).strip()
            orderqty = str(p[5]).strip()
            issuedqty = str(p[6]).strip()
            pltype = str(p[7]).strip()
            shipplno = str(p[8]).strip()
            unit = str(p[9]).strip()
            issuingstatus = str(p[10]).strip()
            uuid = str(p[11]).strip()
            createdby = str(p[12]).strip()
            modifedby = str(p[13]).strip()
            splorder = str(p[14]).strip()
            plnum = pltype + shipplno
            
            sql_fticket_duplicate = f"SELECT ISSUINGKEY FROM TXP_ISSPACKDETAIL WHERE ISSUINGKEY='{issuingkey}' AND FTICKETNO='{fticketno}'"
            Oracur.execute(sql_fticket_duplicate)
            ftdb = Oracur.fetchone()
            
            sql_fticket_insert = f"""UPDATE TXP_ISSPACKDETAIL SET UPDDTE=sysdate WHERE ISSUINGKEY='{issuingkey}' AND FTICKETNO='{fticketno}'"""
            if ftdb is None:
                sql_fticket_insert = f"""INSERT INTO TXP_ISSPACKDETAIL(ISSUINGKEY, PONO, TAGRP, PARTNO, FTICKETNO, ORDERQTY, ISSUEDQTY, SHIPPLNO, UNIT, ISSUINGSTATUS,UPDDTE, SYSDTE, UUID, CREATEDBY, MODIFEDBY,SPLORDER)VALUES('{issuingkey}', '{pono}', '{tagrp}', '{partno}', '{fticketno}', {orderqty}, 0, '{plnum}', '{unit}', 0,current_timestamp, current_timestamp, '{uuid}', '{createdby}', '{modifedby}','{splorder}')"""
                
            Oracur.execute(sql_fticket_insert)
        
        
if __name__ == '__main__':
    main()
    Oracon.commit()
    pgdb.commit()
    pgdb.close()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)