from datetime import datetime
import shutil
import sys
import os
import psycopg2 as pgsql
from nanoid import generate
from spllibs import SplApi
from dotenv import load_dotenv
load_dotenv()


DB_HOSTNAME = os.environ.get('DATABASE_URL')
DB_PORT = os.environ.get('DATABASE_PORT')
DB_NAME = os.environ.get('DATABASE_NAME')
DB_USERNAME = os.environ.get('DATABASE_USERNAME')
DB_PASSWORD = os.environ.get('DATABASE_PASSWORD')

SPL_API_HOST=os.environ.get('SPL_SITE_URL')
SPL_API_USERNAME=os.environ.get('SPL_USERNAME')
SPL_API_PASSWORD=os.environ.get('SPL_PASSWORD')
spl = SplApi(SPL_API_HOST, SPL_API_USERNAME, SPL_API_PASSWORD)

pg_db = pgsql.connect(
    host=DB_HOSTNAME,
    port=DB_PORT,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    database=DB_NAME,
)

pg_cursor = pg_db.cursor()

def get_av00():
    # where t.is_completed=true and t.invoice_status='N'
    sql = f"""select t.id,'AV00' rec_id_a,tft.factory_prefix ||t.inv_prefix inv_prefix,substr(to_char(t.ship_date, 'YYYYMMDD'), 3, 1),to_char(t.running_seq, '0000')  AVIVNO,to_char(t.ship_date, 'YYYYMMDD')  AVIVDT,ta.aff_code AVSHPC,ts.prefix_code AVABT,ta.aff_code AVBLTN,o.pc AVPC,o.commercial AVCOMM,'.' AVVSL1,'.' AVPTRM,t.zone_code AVZONE,'.' AVSHPF,'.' AVSHPT,'.' AVVIA,'000' AVTILE
        from tbt_invoices t
        inner join tbt_orders o on t.order_id = o.id
        inner join tbt_consignees tc on o.consignee_id=tc.id 
        inner join tbt_customers ttc on tc.customer_id=ttc.id
        inner join tbt_affiliates ta on tc.aff_id=ta.id
        inner join tbt_shippings ts on o.shipping_id=ts.id
        inner join tbt_factory_types tft on tc.factory_id=tft.id
        where t.is_completed=true and is_send_gedi=false
        order by t.ship_date,t.running_seq"""
    
    print("1.")
    # print(sql) 
    pg_cursor.execute(sql)
    db = pg_cursor.fetchall()
    doc = []
    for i in db:
        doc.append({
            "primary_id": (str(i[0]).strip()),
            "rec_id_a": (str(i[1]).strip()).ljust(100)[:4],
            "avivno": (f"{str(i[2]).strip()}{str(i[3]).strip()}{str(i[4]).strip()}{str(i[7]).strip()}").ljust(100)[:10],
            "avivdt": (str(i[5]).strip()).ljust(100)[:8],
            "avshpc": (str(i[6]).strip()).ljust(100)[:8],
            "avabt": (str(i[7]).strip()).ljust(100)[:1],
            "avbltn": (str(i[8]).strip()).ljust(100)[:8],
            "avpc": (str(i[9]).strip()).ljust(100)[:1],
            "avcomm": (str(i[10]).strip()).ljust(100)[:1],
            "avvsl1": (str(i[11]).strip()).ljust(100)[:30],
            "avptrm": (str(i[12]).strip()).ljust(100)[:2],
            "avzone": (str(i[13]).strip()).ljust(100)[:10],
            "avshpf": (str(i[14]).strip()).ljust(100)[:15],
            "avshpt": (str(i[15]).strip()).ljust(100)[:15],
            "avvia": (str(i[16]).strip()).ljust(100)[:15],
            "avtile": (str(i[17]).strip()).ljust(100)[:3]
        })
    
    return doc

def get_av01(inv_id):
    sql = f"""select order_id,rec_id_b,avnt01,avnt02,avnt03,avctnt,avflg2,avrgop,avrgdt,avrgtm,avfgsd from (
            select o.id order_id,'AV01' rec_id_b,'.'  AVNT01,'LOADING AT ' || t.loading_area AVNT02,
                case 
                    when t.loading_area ='CK-1' then 'DOMESTIC' 
                    when t.loading_area ='CK-2' then 'BONDED'
                    else '.' 
                end AVNT03,
                to_char(tdd.ctn, '000000000') AVCTNT,'1' AVFLG2,to_char(cast(tu.empcode as integer), '000000')  AVRGOP,to_char(t.created_at, 'YYYYMMDD')  AVRGDT,to_char(t.created_at, 'HHMMSS') AVRGTM, '' AVFGSD
                from tbt_invoices t
                inner join tbt_orders o on t.order_id = o.id
                inner join tbt_consignees tc on o.consignee_id=tc.id 
                inner join tbt_customers ttc on tc.customer_id=ttc.id
                inner join tbt_affiliates ta on tc.aff_id=ta.id
                inner join tbt_shippings ts on o.shipping_id=ts.id
                inner join tbt_territories tt on tc.id=tt.consignee_id 
                inner join tbt_users tu on tt.user_id=tu.id 
                inner join (
                    select dd.order_id,sum(dd.order_balqty/dd.order_stdpack) ctn  from tbt_order_details dd group by dd.order_id
                ) tdd on o.id=tdd.order_id
                where t.id='{inv_id}'
        ) as a
        group by order_id,rec_id_b,avnt01,avnt02,avnt03,avctnt,avflg2,avrgop,avrgdt,avrgtm,avfgsd"""
    
    print("2.")
    # print(sql)
    pg_cursor.execute(sql)
    db = pg_cursor.fetchall()
    doc = []
    for i in db:
        doc.append({
            "rec_id_b": (str(i[1]).strip().ljust(100))[:4],
            "avnt01": (str(i[2]).strip().ljust(100))[:50],
            "avnt02": (str(i[3]).strip().ljust(100))[:50],
            "avnt03": (str(i[4]).strip().ljust(100))[:50],
            "avctnt": (str(i[5]).strip().ljust(100))[:9],
            "avflg2": (str(i[6]).strip().ljust(100))[:1],
            "avrgop": ("SP" + str(i[7]).strip().ljust(100))[:10],
            "avrgdt": (str(i[8]).strip().ljust(100))[:8],
            "avrgtm": (str(i[9]).strip().ljust(100))[:6],
            "avfgsd": (str(i[10]).strip().ljust(100))[:1]
        })
    
    return doc

def get_aw00(inv_id, inv_no):
    sql = f"""select tipp.id pl_id,'AW00' rece_id_aw,'{inv_no}' AWIVNO,ta.aff_code AWAC,t.zone_code AWZONE,tipp.partno AWYPAT,tipp.pono AWODPO,to_char(tipp.bistdp, '0000000') AWPAKQ,to_char(tipp.pallet_no, '000')  AWAETC,to_char(tipp.fcount, '00000') AWCTN,to_char(tipp.fcount*tipp.bistdp, '000000000') AWSPNQ,ts.prefix_code AWISFL,to_char(cast(ttt.empcode as integer), '000000') AWRGOP,to_char(tipp.created_at, 'YYYYMMDD') AWRGDT,to_char(tipp.created_at, 'hh24miss') AWRGTM,tipp.plname
    from tbt_invoices t
    inner join tbt_orders o on t.order_id = o.id
    inner join tbt_consignees tc on o.consignee_id=tc.id 
    inner join (
        select tt.consignee_id,tu.empcode from tbt_territories tt
		inner join tbt_users tu on tt.user_id=tu.id
		group by tt.consignee_id,tu.empcode
    ) as ttt on ttt.consignee_id=tc.id
    inner join tbt_customers ttc on tc.customer_id=ttc.id
    inner join tbt_affiliates ta on tc.aff_id=ta.id
    inner join tbt_shippings ts on o.shipping_id=ts.id
    inner join (
        select tip.id,tip.invoice_id,tip.pallet_no,tp.partno,tp.pono,tp.bistdp,tipp.fcount,tip.pallet_total,tip.created_at,tpop.pallet_width,tpop.pallet_length,tpop.pallet_height,case when tpt.name = 'PALLET' then 'P' else 'C' end plname from tbt_invoice_pallets tip 
        inner join tbt_invoice_pallet_details tipd on tip.id=tipd.invoice_pallet_id 
        inner join tbt_order_details tod on tipd.invoice_part_id=tod.id
        inner join tbt_order_plans tp on tod.order_plan_id=tp.id
        inner join tbt_pallet_types tpt on tip.pallet_type_id=tpt.id 
        left join tbt_placing_on_pallets tpop on tip.placing_id=tpop.id
        inner join (
            select tf.invoice_pallet_detail_id,count(tf.invoice_pallet_detail_id) fcount  from tbt_ftickets tf group by tf.invoice_pallet_detail_id
        ) tipp on tipp.invoice_pallet_detail_id=tipd.id
        group by tip.id,tip.invoice_id,tip.pallet_no,tp.partno,tp.pono,tp.bistdp,tipp.fcount,tip.pallet_total,tip.created_at,tpop.pallet_width,tpop.pallet_length,tpop.pallet_height,tpt.name
        order by tip.pallet_no,tp.partno,tp.pono
    ) tipp on t.id=tipp.invoice_id
    where tipp.invoice_id='{inv_id}'
    order by tipp.pallet_no,tipp.partno,tipp.pono"""
    
    print("3.")
    # print(sql)
    pg_cursor.execute(sql)
    db = pg_cursor.fetchall()
    doc = []
    for i in db:
        doc.append({
            "rece_id_aw": (str(i[1]).strip()).ljust(100)[:4],
            "awivno": (str(i[2]).strip()).ljust(100)[:10],
            "awac": (str(i[3]).strip()).ljust(100)[:8],
            "awzone": (str(i[4]).strip()).ljust(100)[:10],
            "awypat": (str(i[5]).strip()).ljust(100)[:25],
            "awodpo": (str(i[6]).strip()).ljust(100)[:15],
            "awpakq": str(i[7]).strip(),
            "awaetc": str(f"1{str(i[15]).strip()}{str(i[8]).strip()}").ljust(100)[:12],
            "awctn": str(i[9]).strip(),
            "awspnq": str(i[10]).strip(),
            "awisfl": str(i[11]).strip(),
            "awrgop": ("SP"+str(i[12]).strip()).ljust(100)[:10],
            "awrgdt": (str(i[13]).strip()).ljust(100)[:8],
            "awrgtm": (str(i[14]).strip()).ljust(100)[:6]
        })
    
    return doc

def get_bh00(inv_id, inv_no):
    sql = f"""select 'BH00' rec_bh_id, ta.aff_code BHAC, '{inv_no}' BHIVNO, to_char(t.ship_date, 'YYYYMMDD')  BHIVDT, to_char(tip.pallet_no, '000') BHAETC, top.partno  BHYPAT,case when tctc.lot_no is null then '.' else tctc.lot_no end BHLOT, to_char(0, '000') BHCOIL,case when tctc.serial_no is null then '.' else tctc.serial_no end BHSNNO, top.partname  BHDESC, top.pono BHODPO, 'SPLUSER' BHRGOP, to_char(tip.created_at, 'YYYYMMDD') BHRGDT, to_char(tip.created_at, 'hh24miss') BHRGTM, tpop.pallet_width,tpop.pallet_length,tpop.pallet_height,top.biwidt,top.bileng,top.bihigh,top.bigrwt,top.binewt,case when tpt.name='PALLET' then 'P' else 'C' end pltype
        from tbt_invoices t
        inner join tbt_orders o on t.order_id = o.id
        inner join tbt_consignees tc on o.consignee_id=tc.id 
        inner join tbt_customers ttc on tc.customer_id=ttc.id
        inner join tbt_affiliates ta on tc.aff_id=ta.id
        inner join tbt_shippings ts on o.shipping_id=ts.id
        inner join tbt_invoice_pallets tip on t.id=tip.invoice_id
        inner join tbt_invoice_pallet_details tipd on tip.id=tipd.invoice_pallet_id 
        inner join tbt_order_details tod on o.id=tod.order_id and tipd.invoice_part_id=tod.id 
        inner join tbt_order_plans top on tod.order_plan_id=top.id
        inner join tbt_ftickets tf on tipd.id =tf.invoice_pallet_detail_id
        left join tbt_cartons tctc on tf.carton_id=tctc.id
        left join tbt_placing_on_pallets tpop on tip.placing_id=tpop.id
        inner join tbt_pallet_types tpt on tip.pallet_type_id=tpt.id
        where tip.invoice_id='{inv_id}'
        order by tip.pallet_no,top.partno,top.pono,tf.seq,tf.fticket_no"""
    
    print("4.")
    # print(sql)
    pg_cursor.execute(sql)
    db = pg_cursor.fetchall()
    doc = []
    for i in db:
        doc.append({
            "rec_bh_id": (str(i[0]).strip().ljust(50))[:4],
            "bhac": (str(i[1]).strip().ljust(50))[:8],
            "bhivno": (str(i[2]).strip().ljust(50))[:10],
            "bhivdt": (str(i[3]).strip().ljust(50))[:8],
            "bhaetc": ("1"+(f"{str(i[22]).strip()}{str(i[4]).strip()}").ljust(50))[:12],
            "bhypat": (str(i[5]).strip().ljust(25))[:25],
            "bhlot": (str(i[6]).strip().ljust(25))[:8],
            "bhcoil": (str(i[7]).strip().ljust(25))[:3],
            "bhsnno": (str(i[8]).strip().ljust(25))[:18],
            "bhdesc": (str(i[9]).strip().ljust(50))[:25],
            "bhodpo": (str(i[10]).strip().ljust(100))[:15],
            "bhrgop": (str(i[11]).strip().ljust(25))[:10],
            "bhrgdt": (str(i[12]).strip().ljust(25))[:8],
            "bhrgtm": (str(i[13]).strip().ljust(25))[:6]
        })
        
    return doc

def get_br00(inv_id, inv_no):
    sql = f"""select rec_bh_id,brivno,plno,brline,brdesc,to_char(brwidt, '0000'),to_char(brleng, '0000'),to_char(brhigh, '0000'),brqtyp,brcubi,brunit,brrgop,brrgdt,brrgtm,pltype,count(fticket_no) ctn from ( 
        select 'BR00' rec_bh_id,'{inv_no}' BRIVNO,tip.pallet_no plno,'.' BRLINE,'.' BRDESC,tf.fticket_no,case when tpt.name='PALLET' then tpop.pallet_width/10 else top.biwidt end BRWIDT,case when tpt.name='PALLET' then tpop.pallet_length/10 else top.bileng end BRLENG,case when tpt.name='PALLET' then tpop.pallet_height/10 else top.bihigh end BRHIGH,'' BRQTYP,to_char(0, '00000') BRCUBI,'CM/P.' BRUNIT,'SPLUSER' BRRGOP,to_char(tf.updated_at, 'YYYYMMDD') BRRGDT,to_char(tf.updated_at, 'hh24miss') BRRGTM,top.bigrwt/1000 grwt,top.binewt/1000 newt,case when tpt.name='PALLET' then 'P' else 'C' end pltype
        from tbt_invoices t
        inner join tbt_orders o on t.order_id = o.id
        inner join tbt_consignees tc on o.consignee_id=tc.id
        inner join tbt_customers ttc on tc.customer_id=ttc.id
        inner join tbt_affiliates ta on tc.aff_id=ta.id
        inner join tbt_shippings ts on o.shipping_id=ts.id
        inner join tbt_invoice_pallets tip on t.id=tip.invoice_id
        inner join tbt_invoice_pallet_details tipd on tip.id=tipd.invoice_pallet_id
        inner join tbt_order_details tod on o.id=tod.order_id and tipd.invoice_part_id=tod.id
        inner join tbt_order_plans top on tod.order_plan_id=top.id
        inner join tbt_ftickets tf on tipd.id =tf.invoice_pallet_detail_id
        left join tbt_placing_on_pallets tpop on tip.placing_id=tpop.id
        inner join tbt_pallet_types tpt on tip.pallet_type_id=tpt.id
        where tip.invoice_id='{inv_id}'
        order by tip.pallet_no,top.partno,top.pono,tf.seq,tf.fticket_no
    ) as f
    group by rec_bh_id,plno,brivno,brline,brdesc,brwidt,brleng,brhigh,brqtyp,brcubi,brunit,brrgop,brrgdt,brrgtm,pltype"""
    print("5.")
    pg_cursor.execute(sql)
    db = pg_cursor.fetchall()
    doc = []
    brline = 0
    check_dimen = None
    pl_start = None
    brdesc = None
    brqtyp = 0
    r = 0
    for i in db:
        pl = int(str(i[2]).strip())
        if check_dimen is None:
            check_dimen =  str(i[5]).strip()+str(i[6]).strip()+str(i[7]).strip()
            pl_start = pl
            
        else:
            if check_dimen != str(i[5]).strip()+str(i[6]).strip()+str(i[7]).strip():
                if pl_start != pl:
                    # print(f"PL {pl_start}-{(pl - 1)}")
                    brdesc = f"{pl_start}-{(pl - 1)}" 
                    brqtyp = (pl-1)-pl_start
                        
                check_dimen = str(i[5]).strip()+str(i[6]).strip()+str(i[7]).strip()
                pl_start = pl    
        
        r += 1
        if r == len(db):
            if pl_start != pl:
                # print(f"PL {pl_start}-{pl}")
                brdesc = f"{pl_start}-{pl}"
                brqtyp = pl-pl_start
        
        # print(f"{r} db: {len(db)}")
        if brdesc != None:
            brline += 1
            print(f"plstart: {pl_start} dim: {check_dimen} brline: {brline} brdesc: {brdesc} brqtyp: {brqtyp + 1}")
            doc.append({
                "rec_bh_id": (str(i[0]).strip().ljust(50))[:4],
                "brivno": (str(i[1]).strip().ljust(50))[:10],
                "brline": '{0:02}'.format(brline),
                "brdesc": ((f"{str(i[14]).strip()}/NO.{brdesc}").ljust(50))[:50],
                "brwidt": str(i[5]).strip(),
                "brleng": str(i[6]).strip(),
                "brhigh": str(i[7]).strip(),
                "brqtyp": '{0:03}'.format(brqtyp + 1),
                "brcubi": str(i[9]).strip(),
                "brunit": (str(i[10]).strip().ljust(50))[:10],
                "brrgop": (str(i[11]).strip().ljust(50))[:10],
                "brrgdt": str(i[12]).strip(),
                "brrgtm": str(i[13]).strip()
            })
            
        brdesc = None
        
    return doc
    
def main():
    try:
        av00 = get_av00()
        i = 0
        while i < len(av00):
            r = av00[i]
            txt_filename = f'{r["avivno"]}.txt'
            if os.path.isfile(txt_filename):
                os.remove(txt_filename)
                
            f = open(txt_filename, 'a+', encoding='utf-8')
            f.write(f'{r["rec_id_a"]}{r["avivno"]}{r["avivdt"]}{r["avshpc"]}{r["avabt"]}{r["avbltn"]}{r["avpc"]}{r["avcomm"]}{r["avvsl1"]}{r["avptrm"]}{r["avzone"]}{r["avshpf"]}{r["avshpt"]}{r["avvia"]}{r["avtile"]}\n')
            
            avrgop = None
            av01 = get_av01(r["primary_id"])
            for x in av01:
                avrgop = x['avrgop']
                f.write(f"{x['rec_id_b']}{x['avnt01']}{x['avnt02']}{x['avnt03']}{x['avctnt']}{x['avflg2']}{avrgop}{x['avrgdt']}{x['avrgtm']}{x['avfgsd']}\n")
            
            aw00 = get_aw00(r["primary_id"], r["avivno"])
            for x in aw00:
                f.write(f"{x['rece_id_aw']}{x['awivno']}{x['awac']}{x['awzone']}{x['awypat']}{x['awodpo']}{x['awpakq']}{x['awaetc']}{x['awctn']}{x['awspnq']}{x['awisfl']}{avrgop}{x['awrgdt']}{x['awrgtm']}\n")
            
            bh00 = get_bh00(r["primary_id"], r["avivno"])
            for x in bh00:
                f.write(f"{x['rec_bh_id']}{x['bhac']}{x['bhivno']}{x['bhivdt']}{x['bhaetc']}{x['bhypat']}{x['bhlot']}{x['bhcoil']}{x['bhsnno']}{x['bhdesc']}{x['bhodpo']}{avrgop}{x['bhrgdt']}{x['bhrgtm']}\n")
            
            br00 = get_br00(r["primary_id"], r["avivno"])
            for x in br00:
                f.write(f"{x['rec_bh_id']}{x['brivno']}{x['brline']}{x['brdesc']}{x['brwidt']}{x['brleng']}{x['brhigh']}{x['brqtyp']}{x['brcubi']}{x['brunit']}{avrgop}{x['brrgdt']}{x['brrgtm']}\n")
                
            f.close()
            dist = os.path.join(os.path.dirname(__file__), f"BACKUP/UPLOAD/{txt_filename}")
            if os.path.isfile(dist):
                os.remove(dist)
                
            shutil.move(txt_filename, dist)
            pg_cursor.execute(f"update tbt_invoices set invoice_status='H',is_send_gedi=true where id='{r['primary_id']}'")
            pg_db.commit()
            msg = f"Test export GEDI {txt_filename} is completed!"
            spl.line_notification(msg)
            print(msg)
            i += 1
            
    except Exception as ex:
        print(ex)
        pass

if __name__ == '__main__':
    main()
    pg_db.close()
    sys.exit(0)
