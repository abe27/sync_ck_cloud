import sys
import os
import pandas as pd
import psycopg2 as pgsql
from datetime import datetime
from nanoid import generate

DB_HOSTNAME = os.environ.get('DATABASE_URL')
DB_PORT = os.environ.get('DATABASE_PORT')
DB_NAME = os.environ.get('DATABASE_NAME')
DB_USERNAME = os.environ.get('DATABASE_USERNAME')
DB_PASSWORD = os.environ.get('DATABASE_PASSWORD')

### Initail PostgreSQL Server
pgdb = pgsql.connect(
    host=DB_HOSTNAME,
    port=DB_PORT,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    database=DB_NAME,
)
pg_cursor = pgdb.cursor()

def check_nan(txt):
    if txt == 'nan':return ''
    return txt

def read_invoice(file_name):
    try:
        df = pd.read_excel(f"./Invoice/{file_name}", index_col=None)  
        #NoAffNameCustomerAddress
        data = df.to_dict('records')
        for i in data:
            bhivno = check_nan(str(i['BHIVNO']).strip())
            bhodpo = check_nan(str(i['BHODPO']).strip())
            bhivdt = datetime.strptime(str(i['BHIVDT']).strip(), '%d%m%y')
            bhconn = check_nan(str(i['BHCONN']).strip())
            bhcons = check_nan(str(i['BHCONS']).strip()).replace("'", "''")
            bhsven = check_nan(str(i['BHSVEN']).strip())
            bhshpf = check_nan(str(i['BHSHPF']).strip())
            bhsafn = check_nan(str(i['BHSAFN']).strip())
            bhshpt = check_nan(str(i['BHSHPT']).strip())
            bhfrtn = check_nan(str(i['BHFRTN']).strip())
            bhcon = check_nan(str(i['BHCON']).strip())
            paln = check_nan(str(i['BHPALN']).strip())
            bhpnam = check_nan(str(i['BHPNAM']).strip()).replace("'", "''")
            bhypat = check_nan(str(i['BHYPAT']).strip())
            bhctn = check_nan(str(i['BHCTN']).strip())
            bhwidt = check_nan(str(i['BHWIDT']).strip())
            bhleng = check_nan(str(i['BHLENG']).strip())
            bhhigh = check_nan(str(i['BHHIGH']).strip())
            bhgrwt = check_nan(str(i['BHGRWT']).strip())
            bhcbmt = check_nan(str(i['BHCBMT']).strip())
            
            bhpaln = paln
            if paln != '':
                bhpaln = 'P' + "{:03}".format(int(str(paln).replace('.0','')))
            else:
                bhpaln = '-'
            
            ### get order plan
            sql_order_plan = f"select id from tbt_order_plans t where t.bisafn='{bhsafn}' and t.shiptype='{bhivno[len(bhivno)-1:]}' and  etdtap='{bhivdt}' and  pono='{bhodpo}' and partno='{bhypat}'"
            pg_cursor.execute(sql_order_plan)
            order_plan = pg_cursor.fetchone()
            
            sql_filter_invoice = f"select id from tbt_invoice_checks where bhivno='{bhivno}' and bhodpo='{bhodpo}' and bhivdt='{bhivdt}' and bhpaln='{bhpaln}' and bhypat='{bhypat}' and bhctn='{bhctn}' and file_name='{file_name}'"
            pg_cursor.execute(sql_filter_invoice)
            p = pg_cursor.fetchone()
            txt_order_plan = "not match data"
            if p is None:
                txt_order_plan = "not match data"
                is_matched = 'false'
                sql_check_invoice = f"""insert into tbt_invoice_checks(id,bhivno,bhodpo,bhivdt,bhconn,bhcons,bhsven,bhshpf,bhsafn,bhshpt,bhfrtn,bhcon,bhpaln,bhpnam,bhypat,bhctn,bhwidt,bhleng,bhhigh,bhgrwt,bhcbmt,file_name,is_matched,created_at,updated_at)
                values('{generate(size=36)}','{bhivno}','{bhodpo}','{bhivdt}','{bhconn}','{bhcons}','{bhsven}','{bhshpf}','{bhsafn}','{bhshpt}','{bhfrtn}','{bhcon}','{bhpaln}','{bhpnam}','{bhypat}','{bhctn}','{bhwidt}','{bhleng}','{bhhigh}','{bhgrwt}','{bhcbmt}','{file_name}',{is_matched},current_timestamp,current_timestamp)"""
                if order_plan:
                    is_matched = 'true'
                    txt_order_plan = f"match data id: {order_plan[0]}"
                    order_plan_id = order_plan[0]
                    sql_check_invoice = f"""insert into tbt_invoice_checks(id,order_plan_id,bhivno,bhodpo,bhivdt,bhconn,bhcons,bhsven,bhshpf,bhsafn,bhshpt,bhfrtn,bhcon,bhpaln,bhpnam,bhypat,bhctn,bhwidt,bhleng,bhhigh,bhgrwt,bhcbmt,file_name,is_matched,created_at,updated_at)
                    values('{generate(size=36)}','{order_plan_id}','{bhivno}','{bhodpo}','{bhivdt}','{bhconn}','{bhcons}','{bhsven}','{bhshpf}','{bhsafn}','{bhshpt}','{bhfrtn}','{bhcon}','{bhpaln}','{bhpnam}','{bhypat}','{bhctn}','{bhwidt}','{bhleng}','{bhhigh}','{bhgrwt}','{bhcbmt}','{file_name}',{is_matched},current_timestamp,current_timestamp)"""
                
                pg_cursor.execute(sql_check_invoice)
            print(txt_order_plan)
            
            
    except Exception as e:
        print(str(e))
        
def main():
    list_file = os.listdir("./Invoice")
    i = 0
    while i < len(list_file):
        read_invoice(list_file[i])
        i += 1
        
def create_orders(orderid, invoice_no, last_running_no):
    list_order = str(orderid).replace('[', '').replace(']', '')
    sql = f"""select etdtap,vendor,bioabt,biivpx,biac,bishpc,bisafn,shiptype,ordertype,pc,commercial,order_group,is_active,0 items,0 ctn,rcd from (
                select etdtap,vendor,bioabt,biivpx,biac,bishpc,bisafn,shiptype,'-' ordertype,pc,commercial,order_group,is_active,count(partno) items,round(sum(balqty/bistdp))  ctn,
                case when length(trim(substr(reasoncd, 1, 1))) = 0 then '-' else case when trim(substr(reasoncd, 1, 1)) in ('0', 'M') then 'M' else '-' end end rcd
                from tbt_order_plans
                where id in ({list_order})
                group by etdtap,vendor,bioabt,biivpx,biac,bishpc,bisafn,shiptype,pc,commercial,order_group,is_active,substr(reasoncd, 1, 1) 
                order by etdtap,vendor,bioabt,biivpx,biac,bishpc,bisafn,shiptype,pc,commercial,order_group,is_active,substr(reasoncd, 1, 1) 
            ) a
            group by etdtap,vendor,bioabt,biivpx,biac,bishpc,bisafn,shiptype,ordertype,pc,commercial,order_group,is_active,rcd"""
            
    runn_order = 1
    pg_cursor.execute(sql)
    for i in pg_cursor.fetchall():
        etd_date = datetime.strptime(str(i[0]), '%Y-%m-%d')
        vendor = str(i[1])
        bioabt = str(i[2])
        biivpx = str(i[3])
        biac = str(i[4])
        bishpc = str(i[5])
        bisafn = str(i[6])
        shiptype = str(i[7])
        pc = str(i[9])
        commercial = str(i[10])
        order_group = str(i[11])
        filter_by_reason = str(i[15])
        order_type = "-"
        if (filter_by_reason in ["0","M"]):
            order_type = 'M'

        order_whs = "CK-2"
        if (bioabt+vendor) == "1INJ":order_whs = "CK-1"
        if (bioabt+vendor) == "2INJ":order_whs = "NESC"
        if (bioabt+vendor) == "3INJ":order_whs = "ICAM"
        if (bioabt+vendor) == "4INJ":order_whs = "CK-2"
        
        if order_group[:1] =="#":order_whs = "NESC"
        elif order_group[:1] =="@":order_whs = "ICAM"
        
        
        pg_cursor.execute(f"select id from tbt_factory_types where name='{vendor}'")
        factory_id = pg_cursor.fetchone()[0]
        
        pg_cursor.execute(f"select id from tbt_order_zones where factory_id='{factory_id}' and bioat='{bioabt}' and zone='{order_whs}'")
        zname = pg_cursor.fetchone()
        zname_id = generate(size=36)
        sql_zname = f"""insert into tbt_order_zones(id,factory_id,bioat,zone,description,is_active,created_at,updated_at)values('{zname_id}','{factory_id}','{bioabt}','{order_whs}','-',true,current_timestamp,current_timestamp)"""
        if zname:
            zname_id = zname[0]
            sql_zname = f"""update tbt_order_zones set factory_id='{factory_id}',bioat='{bioabt}',zone='{order_whs}',updated_at=current_timestamp where id='{zname_id}'"""
            
        pg_cursor.execute(sql_zname)
        
        pg_cursor.execute(f"select id from tbt_affiliates where aff_code='{biac}'")
        aff = pg_cursor.fetchone()
        ###
        aff_id = generate(size=36)
        sql_insert_aff = f"insert into tbt_affiliates(id,aff_code,description,is_active,created_at,updated_at)values('{aff_id}','{biac}','-',true,current_timestamp,current_timestamp)"
        if aff:
            aff_id = aff[0]
            sql_insert_aff = f"update tbt_affiliates set aff_code='{biac}',updated_at=current_timestamp where id='{aff_id}'"
        pg_cursor.execute(sql_insert_aff)
        
        pg_cursor.execute(f"select id from tbt_customers where cust_code='{bishpc}'")
        customer_id = generate(size=36)
        sql_customer = f"""insert into tbt_customers(id,cust_code,cust_name,description,is_active,created_at,updated_at)values('{customer_id}','{bishpc}','{bisafn}','-',true,current_timestamp,current_timestamp)"""
        bish = pg_cursor.fetchone()
        if bish:
            customer_id = bish[0]
            sql_customer = f"""update tbt_customers set cust_name='{bisafn}',updated_at=current_timestamp where id='{customer_id}'"""
            
        pg_cursor.execute(sql_customer)
        
        pg_cursor.execute(f"select id from tbt_shippings where prefix_code='{shiptype}'")
        shipping_id = pg_cursor.fetchone()[0]
        
        ## get consignee
        pg_cursor.execute(f"select id from tbt_consignees where factory_id='{factory_id}' and aff_id='{aff_id}' and customer_id='{customer_id}'")
        consignee = pg_cursor.fetchone()
        consignee_id = generate(size=36)
        sql_consignee = f"""insert into tbt_consignees(id, factory_id, aff_id, customer_id, prefix_code, last_running_no, group_by,is_active, created_at, updated_at)values('{consignee_id}', '{factory_id}', '{aff_id}', '{customer_id}', '{biivpx}', 1, 'N',true,current_timestamp,current_timestamp)"""
        if consignee:
            consignee_id = consignee[0]
            sql_consignee = f"""update tbt_consignees set updated_at=current_timestamp where id='{consignee_id}'"""
            
        pg_cursor.execute(sql_consignee)
        sql_order = f"""select id from tbt_orders where consignee_id='{consignee_id}' and shipping_id='{shipping_id}' and etd_date='{etd_date}' and order_group='{order_group}' and pc='{pc}' and commercial='{commercial}' and order_type='{order_type}' and bioabt='{bioabt}' and order_whs_id='{zname_id}' and is_invoice=false"""
        #### check order
        order_id = generate(size=36)
        # print(sql_order)
        pg_cursor.execute(sql_order)
        orders = pg_cursor.fetchone()
        sql_insert_order = f"""insert into tbt_orders(id,consignee_id,shipping_id,etd_date,order_group,pc,commercial,order_type,bioabt,order_whs_id,is_matched,is_checked,sync,is_active,created_at,updated_at)
        values('{order_id}','{consignee_id}','{shipping_id}','{etd_date}','{order_group}','{pc}','{commercial}','{order_type}','{bioabt}','{zname_id}',true,true,false,true,current_timestamp,current_timestamp)"""
        if orders:
            order_id = orders[0]
            sql_insert_order = f"update tbt_orders set shipping_id='{shipping_id}',etd_date='{etd_date}',order_whs_id='{zname_id}',is_matched=true,is_checked=true,sync=false,is_active=true,updated_at=current_timestamp where id='{order_id}'"
            if filter_by_reason == "S":
                sql_insert_order = f"update tbt_orders set shipping_id='{shipping_id}',order_whs_id='{zname_id}',is_matched=true,is_checked=true,sync=false,is_active=true,updated_at=current_timestamp where id='{order_id}'"
                
            elif filter_by_reason == "D":
                sql_insert_order = f"update tbt_orders set etd_date='{etd_date}',order_whs_id='{zname_id}',is_matched=true,is_checked=true,sync=false,is_active=true,updated_at=current_timestamp where id='{order_id}'"
            
        pg_cursor.execute(sql_insert_order)
        # sql_reason = "and substring(reasoncd, 1, 1) not in ('M', '0')"
        # if order_type == 'M':sql_reason = "and substring(reasoncd, 1, 1) in ('M', '0')"
        sql_body = f"""select '{order_id}' order_id,id order_plan_id,case when length(reasoncd) > 0 then reasoncd else '-' end revise_id,partno ledger_id,pono,lotno,ordermonth,orderorgi,orderround,balqty,bistdp,shippedflg,shippedqty,sampleflg,carriercode,bidrfl,deleteflg  delete_flg,firmflg  firm_flg,'' poupd_flg,unit,partname 
        from tbt_order_plans 
        where id in (select order_plan_id  from tbt_invoice_checks where bhivno='{invoice_no}')
        order by created_at,sequence"""
        # print(sql_body)
        pg_cursor.execute(sql_body)
        db = pg_cursor.fetchall()
        
        runn = 1
        print(f"START ================== {runn_order} ======================")
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
            pg_cursor.execute(f"select id from tbt_order_revises where value='{reason_cd}'")
            revise = pg_cursor.fetchone()
            if revise is None:
                revise_insert = f"insert into tbt_order_revises(id,name,value,description,new_or_revise,is_active,created_at,updated_at)values('{revise_id}','{reason_cd}','{reason_cd}','-',false,true,current_timestamp,current_timestamp)"
                pg_cursor.execute(revise_insert)
                
            else:
                revise_id = revise[0]
            
            p = "PART"
            if part_no[:1] == '1':p = "WIRE"
            pg_cursor.execute(f"select id from tbt_part_types where name='{p}'")
            part_type_id = pg_cursor.fetchone()[0]
            pg_cursor.execute(f"select id from tbt_tagrps where name='C'")
            tagrp_id = pg_cursor.fetchone()[0]
            pg_cursor.execute(f"select id from tbt_whs where name='CK-2'")
            whs_id = pg_cursor.fetchone()[0]
            
            pg_cursor.execute(f"select id from tbt_parts where no='{part_no}'")
            part_id = generate(size=36)
            part = pg_cursor.fetchone()
            sql_part = f"insert into tbt_parts(id,no,name,is_active,created_at,updated_at)values('{part_id}','{part_no}','{partname}',true,current_timestamp,current_timestamp)"
            if part:
                part_id = part[0]
                sql_part = f"update tbt_parts set name='{partname}',updated_at=current_timestamp where id='{part_id}'"
                
            pg_cursor.execute(sql_part)
            
            pg_cursor.execute(f"select id from tbt_units where name='{unit}'")
            unit_id = pg_cursor.fetchone()[0]
            
            pg_cursor.execute(f"select id from tbt_ledgers where factory_id='{factory_id}' and part_id='{part_id}' and whs_id='{whs_id}' and unit_id='{unit_id}'")
            ledger_id = generate(size=36)
            sql_ledger = f"""insert into tbt_ledgers(id,part_type_id,tagrp_id,factory_id,whs_id,part_id,unit_id,is_active,created_at,updated_at)values('{ledger_id}','{part_type_id}','{tagrp_id}','{factory_id}','{whs_id}','{part_id}','{unit_id}',true,current_timestamp,current_timestamp)"""
            ledger = pg_cursor.fetchone()
            if ledger is None:
                pg_cursor.execute(sql_ledger)
                
            else:
                ledger_id = ledger[0]
            
            ### check order detail
            sql_order_detail = f"select id from tbt_order_details where order_id='{order_id}' and ledger_id='{ledger_id}' and pono='{pono}'"
            # print(sql_order_detail)
            txt_sql_order_detail = "insert"
            order_detail_id = generate(size=36)
            pg_cursor.execute(sql_order_detail)
            ord_detail = pg_cursor.fetchone()
            ord_detail_insert = f"""insert into tbt_order_details(id, order_id,order_plan_id,revise_id,ledger_id,pono,lotno,order_month,order_orgi,order_round,order_balqty,order_stdpack,shipped_flg,shipped_qty,sam_flg,carrier_code,bidrfl,delete_flg,reason_code,firm_flg,poupd_flg,is_active,created_at,updated_at)
            values('{order_detail_id}','{order_id}','{order_plan_id}','{revise_id}','{ledger_id}','{pono}','{lotno}','{order_month}','{order_orgi}','{order_round}','{balqty}','{bistdp}','{shippedflg}','{shippedqty}','{sampleflg}','{carriercode}','{bidrfl}','{delete_flg}','{reason_cd}','{firm_flg}','{poupd_flg}',true,current_timestamp,current_timestamp)"""
            if ord_detail:
                txt_sql_order_detail = "update"
                order_detail_id = ord_detail[0]
                ord_detail_insert = f"update tbt_order_details set revise_id='{revise_id}',order_plan_id='{order_plan_id}',order_balqty='{balqty}',order_month='{order_month}',reason_code='{reason_cd}',updated_at=current_timestamp where id='{order_detail_id}'"
            pg_cursor.execute(ord_detail_insert)
            print(f"{txt_sql_order_detail} ==> {runn} etd: {etd_date} ship: {shiptype} order: {pono} part: {part_no} R: {reason_cd} qty: {balqty} stdpack: {bistdp}")
            runn += 1
        
        pg_cursor.execute(f"""update tbt_order_plans set is_generated=true where etdtap='{etd_date}' and vendor='{vendor}' and bioabt='{bioabt}' and biivpx='{biivpx}' and biac='{biac}' and bishpc='{bishpc}' and shiptype='{shiptype}' and pc='{pc}' and commercial='{commercial}' and order_group='{order_group}'""")    
        # pgdb.commit()
        print(f"END ================== {runn_order} ======================")
        runn_order += 1
        print(f"START ------------------ {invoice_no} -----------------")
        ### create invoice
        sql_inv = f"""select d.id,tc.factory_id,d.consignee_id,tc.prefix_code,tc.last_running_no,d.etd_date,toz.zone order_whs_id,tit.id title_id,ts.prefix_code from tbt_orders d
        inner join tbt_consignees tc on d.consignee_id=tc.id 
        inner join tbt_shippings ts on d.shipping_id=ts.id
        inner join tbt_factory_types tft on tc.factory_id=tft.id
        inner join tbt_order_zones toz on d.order_whs_id=toz.id
        inner join tbt_invoice_titles tit on tit.title='000'
        where d.id='{order_id}'
        order by d.etd_date,d.created_at"""
        # print(sql_inv)
        pg_cursor.execute(sql_inv)
        db = pg_cursor.fetchall()
        for i in db:
            order_id = str(i[0])
            factory_id = str(i[1])
            consignee_id = str(i[2])
            prefix_code = str(i[3])
            etd_date = datetime.strptime(str(i[5]), '%Y-%m-%d')
            order_whs_id = str(i[6])
            title_id = str(i[7])
            shiptype = str(i[8]).strip()
            privilege = "EXPORT"
            
            
            loading_area = "CK-2"
            end_zname = shiptype
            if shiptype == "T":
                end_zname = "I"
                loading_area = "CK-1"
                # privilege = "DOMESTIC"
                
            if order_whs_id == "CK-2":
                end_zname = "C"
                    
            elif order_whs_id == "NESC":
                end_zname = "N"
                loading_area = "CK-1"
                
            elif order_whs_id == "ICAM":
                end_zname = "I"
                loading_area = "CK-1"
                
            zone_code_last = f"{str(etd_date.strftime('%Y%m%d'))[3:]}{end_zname}"
            pg_cursor.execute(f"select count(*) + 1 from tbt_invoices where zone_code like '{zone_code_last}%'")
            last_zone_running = int(str(pg_cursor.fetchone()[0]))
            zone_code = f"{str(etd_date.strftime('%Y%m%d'))[3:]}{end_zname}{'{:03d}'.format(last_zone_running)}"
            
            pg_cursor.execute(f"select id from tbt_whs where name='{order_whs_id}'")
            whs_id = pg_cursor.fetchone()[0]
            
            #### get references_id
            pg_cursor.execute(f"select count(*) + 1 from tbt_invoices where references_id like 'S{prefix_code}-{str(etd_date.strftime('%Y%m%d'))}%'")
            last_ref_running = int(str(pg_cursor.fetchone()[0]))
            references_no = f"S{prefix_code}-{str(etd_date.strftime('%Y%m%d'))}-{'{:04d}'.format(last_ref_running)}"
            
            #### create invoice
            sql_check_order = f"select id from tbt_invoices where order_id='{order_id}'"
            pg_cursor.execute(sql_check_order)
            inv = pg_cursor.fetchone()
            ship_der = "LCL"
            if shiptype == "T":ship_der="TRUCK"
            elif shiptype == "A":ship_der="AIR"
            
            inv_id = generate(size=36)
            sql_insert_invoice = f"""insert into tbt_invoices(id, order_id, inv_prefix, running_seq, ship_date, ship_from_id, ship_via, ship_der, title_id, loading_area, privilege, zone_code,references_id, invoice_status, is_active, created_at, updated_at)
            values('{inv_id}', '{order_id}', '{prefix_code}', {last_running_no}, '{etd_date}', '{whs_id}', '-', '{ship_der}', '{title_id}', '{loading_area}', '{privilege}', '{zone_code}','{references_no}','N', true, current_timestamp, current_timestamp)"""
            if inv:
                inv_id=inv[0]
                sql_insert_invoice = f"update tbt_invoices set order_id='{order_id}',ship_der='{ship_der}' where id='{inv_id}'"
            
            pg_cursor.execute(sql_insert_invoice)
            pg_cursor.execute(f"update tbt_consignees set last_running_no='{last_running_no}' where prefix_code ='{prefix_code}' and factory_id='{factory_id}'")
            pg_cursor.execute(f"update tbt_orders set sync=false,is_invoice=true,updated_at=current_timestamp where id='{order_id}'")
            print(f"update data {order_id}")
            
        
            ### create pallet invoice_no
            sql_pallet = f"""select bhivno,order_plan_id,bhctn,bhpaln,case when length(substring(bhpaln, 2, 3)) > 0 then substring(bhpaln, 2, 3) else '0' end from tbt_invoice_checks 
            where order_plan_id is not null and bhivno='{invoice_no}' 
            group by bhivno,order_plan_id,bhctn,bhpaln 
            order by bhivno,bhpaln,order_plan_id,bhctn"""
            pg_cursor.execute(sql_pallet)
            pl = pg_cursor.fetchall()
            invoice_id = inv_id
            pg_cursor.execute(f"select id from tbt_placing_on_pallets where name='ARROW NO.1'")
            placing_id = pg_cursor.fetchone()[0]
            pg_cursor.execute(f"select id from tbt_locations where name='-'")
            location_id = pg_cursor.fetchone()[0]
            pallet_no = 0
            pg_cursor.execute(f"select id from tbt_pallet_types where name='PALLET'")
            pallet_type_id = pg_cursor.fetchone()[0]
            ### for box seq
            pallet_total = 0
            pallet_no_seq = 0
            older_pallet = None
            for p in pl:
                bhivno = str(p[0])
                order_plan_id = str(p[1])
                bhctn = float(str(p[2]))
                bhpaln = str(p[3])
                pallet_no = int(p[4])
                plid = generate(size=36)
                invoice_pallet_detail_id = generate(size=36)
                if older_pallet is None:
                    older_pallet = bhpaln
                    pallet_total = 0
                    
                else:
                    if older_pallet != bhpaln:
                        older_pallet = bhpaln
                        pallet_total = 0
                
                if bhpaln == "-":
                    pg_cursor.execute(f"select id from tbt_pallet_types where name='BOX'")
                    pallet_type_id = pg_cursor.fetchone()[0]
                    pallet_no = 0
                
                else:
                    ### for pallet
                    plno = bhpaln
                    plid = generate(size=36)
                    invoice_pallet_detail_id = generate(size=36)
                    sql_pallet = f"select id from tbt_invoice_pallets where invoice_id='{invoice_id}' and pallet_no='{pallet_no}' and pallet_type_id='{pallet_type_id}'"   
                    pg_cursor.execute(sql_pallet)
                    pl_id = pg_cursor.fetchone()
                    sql_insert_pallet = f"""insert into tbt_invoice_pallets(id, invoice_id, placing_id, location_id, pallet_no, spl_pallet_no, pallet_total, is_active, created_at, updated_at, pallet_type_id)values('{plid}', '{invoice_id}', '{placing_id}', '{location_id}', {pallet_no}, '-', 0, true, current_timestamp, current_timestamp, '{pallet_type_id}')"""
                    if bhpaln == "-":
                        sql_insert_pallet = f"""insert into tbt_invoice_pallets(id, invoice_id, location_id, pallet_no, spl_pallet_no, pallet_total, is_active, created_at, updated_at, pallet_type_id)values('{plid}', '{invoice_id}', '{location_id}', {pallet_no}, '-', 0, true, current_timestamp, current_timestamp, '{pallet_type_id}')"""
                        
                    if pl_id:plid=pl_id[0]
                    else:pg_cursor.execute(sql_insert_pallet)
                    
                    sql_order_details = f"select id from tbt_order_details where order_plan_id='{order_plan_id}'"
                    pg_cursor.execute(sql_order_details)
                    order_details_id = pg_cursor.fetchone()[0]
                    sql_pallet_detail = f"select id from tbt_invoice_pallet_details where invoice_pallet_id='{plid}' and invoice_part_id='{order_details_id}'"
                    pg_cursor.execute(sql_pallet_detail)
                    pallet_detail = pg_cursor.fetchone()
                    sql_pallet_detail_insert = f"""insert into tbt_invoice_pallet_details(id, invoice_pallet_id, is_printed, is_active, created_at, updated_at, invoice_part_id)values('{invoice_pallet_detail_id}', '{plid}', false, true, current_timestamp, current_timestamp, '{order_details_id}')"""
                    if pallet_detail:
                        invoice_pallet_detail_id = pallet_detail[0]
                        sql_pallet_detail_insert = f"""update tbt_invoice_pallet_details set invoice_pallet_id='{plid}',updated_at=current_timestamp,invoice_part_id='{order_details_id}' where id='{invoice_pallet_detail_id}'"""
                        
                    pg_cursor.execute(sql_pallet_detail_insert)
                    pg_cursor.execute(f"update tbt_order_details set set_pallet_ctn={bhctn} where order_plan_id='{order_plan_id}'")
                
                
                ### create fticketno
                d = datetime.now().strftime('%Y')
                x = 0
                while x < bhctn:
                    if bhpaln == "-":
                        ### for box
                        pallet_total = 1
                        pallet_no_seq += 1
                        plno = f"C{'{:03d}'.format(pallet_no_seq)}"
                        plid = generate(size=36)
                        invoice_pallet_detail_id = generate(size=36)
                        sql_pallet = f"select id from tbt_invoice_pallets where invoice_id='{invoice_id}' and pallet_no='{pallet_no_seq}' and pallet_type_id='{pallet_type_id}'"   
                        pg_cursor.execute(sql_pallet)
                        pl_id = pg_cursor.fetchone()
                        sql_insert_pallet = f"""insert into tbt_invoice_pallets(id, invoice_id, placing_id, location_id, pallet_no, spl_pallet_no, pallet_total, is_active, created_at, updated_at, pallet_type_id)values('{plid}', '{invoice_id}', '{placing_id}', '{location_id}', {pallet_no_seq}, '-', {pallet_total}, true, current_timestamp, current_timestamp, '{pallet_type_id}')"""
                        if bhpaln == "-":
                            sql_insert_pallet = f"""insert into tbt_invoice_pallets(id, invoice_id, location_id, pallet_no, spl_pallet_no, pallet_total, is_active, created_at, updated_at, pallet_type_id)values('{plid}', '{invoice_id}', '{location_id}', {pallet_no_seq}, '-', {pallet_total}, true, current_timestamp, current_timestamp, '{pallet_type_id}')"""
                            
                        if pl_id:plid=pl_id[0]
                        else:pg_cursor.execute(sql_insert_pallet)
                        
                        sql_order_details = f"select id from tbt_order_details where order_plan_id='{order_plan_id}'"
                        pg_cursor.execute(sql_order_details)
                        order_details_id = pg_cursor.fetchone()[0]
                        sql_pallet_detail = f"select id from tbt_invoice_pallet_details where invoice_pallet_id='{plid}' and invoice_part_id='{order_details_id}'"
                        pg_cursor.execute(sql_pallet_detail)
                        pallet_detail = pg_cursor.fetchone()
                        sql_pallet_detail_insert = f"""insert into tbt_invoice_pallet_details(id, invoice_pallet_id, is_printed, is_active, created_at, updated_at, invoice_part_id)values('{invoice_pallet_detail_id}', '{plid}', false, true, current_timestamp, current_timestamp, '{order_details_id}')"""
                        if pallet_detail:
                            invoice_pallet_detail_id = pallet_detail[0]
                            sql_pallet_detail_insert = f"""update tbt_invoice_pallet_details set invoice_pallet_id='{plid}',updated_at=current_timestamp,invoice_part_id='{order_details_id}' where id='{invoice_pallet_detail_id}'"""
                            
                        pg_cursor.execute(sql_pallet_detail_insert)
                    
                    
                    sql_fticket = f"select id,running_seq from tbt_fticket_seqs where fticket_prefix='V' and on_year='{d}'"
                    pg_cursor.execute(sql_fticket)
                    f = pg_cursor.fetchone()
                    fticketno = f"V{d[3:]}{'{:08d}'.format(f[1])}"
                    sql_fticket = f"select id from tbt_ftickets where fticket_no='{fticketno}'"
                    pg_cursor.execute(sql_fticket)
                    ftickets = pg_cursor.fetchone()
                    fticket_id = generate(size=36)
                    sql_insert_fticket = f"""insert into tbt_ftickets(id, invoice_pallet_detail_id, fticket_no, description, is_printed, is_scanned, is_active, created_at, updated_at, seq)
                    values('{fticket_id}', '{invoice_pallet_detail_id}', '{fticketno}', '-', false, false, true, current_timestamp, current_timestamp, {(x + 1)})"""
                    if ftickets:
                        fticket_id = ftickets[0]
                        sql_insert_fticket = f"""update tbt_ftickets set updated_at=current_timestamp where id='{fticket_id}'"""
                    
                    pg_cursor.execute(sql_insert_fticket)
                    pg_cursor.execute(f"update tbt_fticket_seqs set running_seq={int(f[1]) + 1} where id='{f[0]}'")
                    if bhpaln != "-":
                        pallet_total += 1
                        sql_update_pallet_total = f"update tbt_invoice_pallets set pallet_total='{pallet_total}' where id='{plid}'"
                        pg_cursor.execute(sql_update_pallet_total)
                    
                    print(f"{x} => {fticketno} pallet: {plno} inv: {bhivno} total: {pallet_total}")
                    x += 1
                    
                # pallet_total += bhctn

            sql_update_invoice_check = f"update tbt_invoice_checks set bhsync=true where bhivno='{invoice_no}'"
            pg_cursor.execute(sql_update_invoice_check)
            pgdb.commit()
            print(f"END --------------------------------------")
        
def create_invoice():
    sql = f"""
    select substring(bhivno, 6, 4) inv,bhivno,order_plan_id from tbt_invoice_checks where order_plan_id is not null and bhsync=false
    group by substring(bhivno, 6, 4),bhivno,order_plan_id
    order by substring(bhivno, 6, 4),bhivno,order_plan_id
    """
    pg_cursor.execute(sql)
    db = pg_cursor.fetchall()
    
    r = 1
    inv_check = None
    orders = []
    for i in db:
        runn_no = int(str(i[0]))
        invoice_no = str(i[1]).strip()
        order_plan_id = str(i[2]).strip()
        
        if inv_check is None:
            inv_check = invoice_no
            
        if inv_check != invoice_no:
            print(f"--------------- end -----------------")
            create_orders(orders, invoice_no, runn_no)
            inv_check = invoice_no
            r = 1
            orders = []
        
        orders.append(order_plan_id)
        print(f"{r}. {inv_check} order id: {order_plan_id}")
        r += 1
        
    print(f"-->")

if __name__ == '__main__':
    # main()
    create_invoice()
    pgdb.commit()
    pgdb.close()
    sys.exit(0)