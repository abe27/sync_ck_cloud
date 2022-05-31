from datetime import datetime
import shutil
import sys
import os
import time
import psycopg2 as pgsql
import cx_Oracle
from nanoid import generate
from dotenv import load_dotenv
load_dotenv()

DB_HOSTNAME=os.environ.get('DATABASE_URL')
DB_PORT=os.environ.get('DATABASE_PORT')
DB_NAME=os.environ.get('DATABASE_NAME')
DB_USERNAME=os.environ.get('DATABASE_USERNAME')
DB_PASSWORD=os.environ.get('DATABASE_PASSWORD')

def main():
    ### Initail Mysql Server
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    
    mycursor = mydb.cursor()
    sql =f"""
    select a.* from (
        select etdtap,vendor,bioabt,biivpx,biac,bishpc,bisafn,bicomd,shiptype,ordertype,pc,commercial,order_group,is_active,count(partno) items,round(sum(balqty/bistdp))  ctn    
        from tbt_order_plans
        where is_generated=false
        group by etdtap,vendor,bioabt,biivpx,biac,bishpc,bisafn,bicomd,shiptype,ordertype,pc,commercial,order_group,is_active
        order by etdtap,vendor,bioabt,biivpx,biac,bishpc,bisafn,bicomd,shiptype,ordertype,pc,commercial,order_group,is_active
    ) a
    where a.ctn > 0
    """
    runn_order = 1
    mycursor.execute(sql)
    for i in mycursor.fetchall():
        etd_date = datetime.strptime(str(i[0]), '%Y-%m-%d')
        vendor = str(i[1])
        bioabt = str(i[2])
        biivpx = str(i[3])
        biac = str(i[4])
        bishpc = str(i[5])
        bisafn = str(i[6])
        bicomd = str(i[7])
        shiptype = str(i[8])
        order_type = str(i[9])
        pc = str(i[10])
        commercial = str(i[11])
        order_group = str(i[12])
        is_active = str(i[13])
        items = str(i[14])
        ctn = str(i[15])
        
        mycursor.execute(f"select id from tbt_factory_types where name='{vendor}'")
        factory_id = mycursor.fetchone()[0]
        
        mycursor.execute(f"select id from tbt_affiliates where aff_code='{biac}'")
        aff = mycursor.fetchone()
        ###
        aff_id = generate(size=36)
        sql_insert_aff = f"insert into tbt_affiliates(id,aff_code,description,is_active,created_at,updated_at)values('{aff_id}','{biac}','-',true,current_timestamp,current_timestamp)"
        if aff:
            aff_id = aff[0]
            sql_insert_aff = f"update tbt_affiliates set aff_code='{biac}',updated_at=current_timestamp where id='{aff_id}'"
        mycursor.execute(sql_insert_aff)
        
        mycursor.execute(f"select id from tbt_customers where cust_code='{bishpc}'")
        customer_id = generate(size=36)
        sql_customer = f"""insert into tbt_customers(id,cust_code,cust_name,description,is_active,created_at,updated_at)values('{customer_id}','{bishpc}','{bisafn}','-',true,current_timestamp,current_timestamp)"""
        bish = mycursor.fetchone()
        if bish:
            customer_id = bish[0]
            sql_customer = f"""update tbt_customers set cust_name='{bisafn}',updated_at=current_timestamp where id='{customer_id}'"""
        mycursor.execute(sql_customer)
        
        mycursor.execute(f"select id from tbt_shippings where prefix_code='{shiptype}'")
        shipping_id = mycursor.fetchone()[0]
        
        ## get consignee
        mycursor.execute(f"select id from tbt_consignees where factory_id='{factory_id}' and aff_id='{aff_id}' and customer_id='{customer_id}'")
        consignee = mycursor.fetchone()
        consignee_id = generate(size=36)
        sql_consignee = f"""insert into tbt_consignees(id, factory_id, aff_id, customer_id, prefix_code, last_running_no, group_by,is_active, created_at, updated_at)values('{consignee_id}', '{factory_id}', '{aff_id}', '{customer_id}', '{biivpx}', 1, 'N',true,current_timestamp,current_timestamp)"""
        if consignee:
            consignee_id = consignee[0]
            sql_consignee = f"""update tbt_consignees set updated_at=current_timestamp where id='{consignee_id}'"""
        mycursor.execute(sql_consignee)
        #### check order
        order_id = generate(size=36)
        mycursor.execute(f"select id from tbt_orders where consignee_id='{consignee_id}' and shipping_id='{shipping_id}' and etd_date='{etd_date}' and order_group='{order_group}' and pc='{pc}' and commercial='{commercial}' and order_type='{order_type}' and bioabt='{bioabt}' and bicomd='{bicomd}'")
        orders = mycursor.fetchone()
        
        sql_insert_order = f"""insert into tbt_orders(id,consignee_id,shipping_id,etd_date,order_group,pc,commercial,order_type,bioabt,bicomd,sync,is_active,created_at,updated_at)
        values('{order_id}','{consignee_id}','{shipping_id}','{etd_date}','{order_group}','{pc}','{commercial}','{order_type}','{bioabt}','{bicomd}',false,false,current_timestamp,current_timestamp)"""
        if orders:
            order_id = orders[0]
            sql_insert_order = f"update tbt_orders set sync=false,updated_at=current_timestamp where id='{order_id}'"
        mycursor.execute(sql_insert_order)
        
        sql_body = f"""select '{order_id}' order_id,id order_plan_id,case when length(reasoncd) > 0 then reasoncd else '-' end revise_id,partno ledger_id,pono,lotno,ordermonth,orderorgi,orderround,balqty,bistdp,shippedflg,shippedqty,sampleflg,carriercode,bidrfl,deleteflg  delete_flg,firmflg  firm_flg,'' poupd_flg,unit,partname from tbt_order_plans where etdtap='{etd_date}' and vendor='{vendor}' and bioabt='{bioabt}' and biivpx='{biivpx}' and biac='{biac}' and bishpc='{bishpc}' and bicomd='{bicomd}' and shiptype='{shiptype}' and ordertype='{order_type}' and pc='{pc}' and commercial='{commercial}' and order_group='{order_group}' and is_active=true order by created_at"""
        mycursor.execute(sql_body)
        db = mycursor.fetchall()
        
        runn = 1
        for r in db:
            order_plan_id = str(r[1])
            reason_cd = str(r[2])
            part_no = str(r[3])
            pono = str(r[4]).strip()
            lotno = str(r[5])
            order_month = str(r[6])
            order_orgi = str(r[7])
            order_round = str(r[8])
            balqty = float(str(r[9]))
            bistdp = float(str(r[10]))
            shippedflg = str(r[11])
            shippedqty = float(str(r[12]))
            sampleflg = str(r[13])
            carriercode = str(r[14])
            bidrfl = str(r[15])
            delete_flg = str(r[16])
            firm_flg = str(r[17])
            poupd_flg = str(r[18])
            unit = str(r[19])
            partname = str(r[20]).replace("'", "'''")
            
            revise_id = generate(size=36)
            mycursor.execute(f"select id from tbt_order_revises where value='{reason_cd}'")
            revise_insert = f"insert into tbt_order_revises(id,name,value,description,new_or_revise,is_active,created_at,updated_at)values('{revise_id}','{reason_cd}','{reason_cd}','-',false,true,current_timestamp,current_timestamp)"
            revise = mycursor.fetchone()
            if revise:
                revise_id = revise[0]
                revise_insert = f"update tbt_order_revises set name='{reason_cd}',updated_at=current_timestamp where id='{revise_id}'"
            mycursor.execute(revise_insert)
            
            p = "PART"
            if part_no[:1] == '1':p = "WIRE"
            mycursor.execute(f"select id from tbt_part_types where name='{p}'")
            part_type_id = mycursor.fetchone()[0]
            mycursor.execute(f"select id from tbt_tagrps where name='C'")
            tagrp_id = mycursor.fetchone()[0]
            mycursor.execute(f"select id from tbt_whs where name='CK-2'")
            whs_id = mycursor.fetchone()[0]
            
            mycursor.execute(f"select id from tbt_parts where no='{part_no}'")
            part_id = generate(size=36)
            part = mycursor.fetchone()
            sql_part = f"insert into tbt_parts(id,no,name,is_active,created_at,updated_at)values('{part_id}','{part_no}','{partname}',true,current_timestamp,current_timestamp)"
            if part:
                part_id = part[0]
                sql_part = f"update tbt_parts set name='{partname}',updated_at=current_timestamp where id='{part_id}'"
                
            mycursor.execute(sql_part)
            
            mycursor.execute(f"select id from tbt_units where name='{unit}'")
            unit_id = mycursor.fetchone()[0]
            
            mycursor.execute(f"select id from tbt_ledgers where factory_id='{factory_id}' and part_id='{part_id}' and whs_id='{whs_id}' and unit_id='{unit_id}'")
            ledger_id = generate(size=36)
            sql_ledger = f"""insert into tbt_ledgers(id,part_type_id,tagrp_id,factory_id,whs_id,part_id,unit_id,is_active,created_at,updated_at)values('{ledger_id}','{part_type_id}','{tagrp_id}','{factory_id}','{whs_id}','{part_id}','{unit_id}',true,current_timestamp,current_timestamp)"""
            ledger = mycursor.fetchone()
            if ledger:
                ledger_id = ledger[0]
                sql_ledger = f"""update tbt_ledgers set is_active=true,updated_at=current_timestamp where id='{ledger_id}'"""
            mycursor.execute(sql_ledger)
            
            ### check order detail
            sql_order_detail = f"select id from tbt_order_details where order_id='{order_id}' and ledger_id='{ledger_id}' and pono='{pono}'"
            # print(sql_order_detail)
            order_detail_id = generate(size=36)
            mycursor.execute(sql_order_detail)
            ord_detail = mycursor.fetchone()
            ord_detail_insert = f"""insert into tbt_order_details(id, order_id,order_plan_id,revise_id,ledger_id,pono,lotno,order_month,order_orgi,order_round,order_balqty,order_stdpack,shipped_flg,shipped_qty,sam_flg,carrier_code,bidrfl,delete_flg,reason_code,firm_flg,poupd_flg,is_active,created_at,updated_at)
            values('{order_detail_id}','{order_id}','{order_plan_id}','{revise_id}','{ledger_id}','{pono}','{lotno}','{order_month}','{order_orgi}','{order_round}','{balqty}','{bistdp}','{shippedflg}','{shippedqty}','{sampleflg}','{carriercode}','{bidrfl}','{delete_flg}','{reason_cd}','{firm_flg}','{poupd_flg}',true,current_timestamp,current_timestamp)"""
            if ord_detail:
                order_detail_id = ord_detail[0]
                ord_detail_insert = f"update tbt_order_details set revise_id='{revise_id}',order_plan_id='{order_plan_id}',order_balqty='{balqty}',order_month='{order_month}',reason_code='{reason_cd}',updated_at=current_timestamp where id='{order_detail_id}'"
            mycursor.execute(ord_detail_insert)
            print(f"{runn} {pono} {part_no} R: {reason_cd} qty: {balqty} stdpack: {bistdp}")
            runn += 1
            
        mydb.commit()
        print(f"{runn_order} ========================================")
        runn_order += 1
        
    mydb.close()
    
if __name__ == '__main__':
    main()
    sys.exit(0)
    