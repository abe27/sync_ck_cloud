from datetime import datetime
import os, sys, psycopg2, pathlib
from bs4 import BeautifulSoup
import urllib
import shutil
import cx_Oracle as o
import requests
from termcolor import colored
import urllib3
from colorama import init
init(autoreset=True)
from dotenv import load_dotenv
env_path = f'{pathlib.Path().absolute()}/.env'
load_dotenv(env_path)

conn = psycopg2.connect(host=os.getenv("DB_HOST"),port=os.getenv("DB_PORT"),database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWD"))
ora = o.connect(os.getenv("ORA_STR"))

def __PsFetchOne(sql):
    i = False
    cur = conn.cursor()
    try:
        cur.execute(sql)
        db = cur.fetchone()
        if db is not None:
            i = str(db[0])
        else:
            i = False

        cur.close()
        conn.commit()
    except Exception as e:
        print(sql)
        print(str(e))
        cur.close()
        conn.rollback()
        sys.exit(0)

    return i

def __PsFetchAll(sql):
    obj = []
    try:
        cur = conn.cursor()
        cur.execute(sql)
        obj = cur.fetchall()
        conn.commit()
    except Exception as ex:
        print(sql)
        print(ex)
        conn.rollback()
        sys.exit(0)

    return obj

def __PsExcute(sql):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
        pass
    except Exception as e:
        print(sql)
        print(str(e))
        conn.rollback()
        sys.exit(0)

    cur.close()
    return True

def __OraFetchOne(sql):
    i = False
    cur = ora.cursor()
    try:
        cur.execute(sql)
        db = cur.fetchone()
        if db is not None:
            i = str(db[0])
        else:
            i = False

        cur.close()
        ora.commit()
    except Exception as e:
        print(sql)
        print(str(e))
        cur.close()
        ora.rollback()
        sys.exit(0)

    return i

def __OraFetchAll(sql):
    cur = ora.cursor()
    cur.execute(sql)
    obj = cur.fetchall()
    return obj

def __OraExcute(sql):
    x = False
    cur = ora.cursor()
    try:
        cur.execute(sql)
        ora.commit()
        x = True
        pass
    except Exception as e:
        print(sql)
        print(str(e))
        ora.rollback()
        sys.exit(0)
        pass

    cur.close()
    return x

def __get_customer(invno):
    sql = f"""
    select
        tc.affcode,tc.consignee,tc.custname,ts.ship_title,ta.cust_company,to_char(p.ord_plan_etdtap,'yyMMdd')||ts.ship_title||'1' zonecode
    from tbt_invoiceinfomations f
    inner join tbt_invoiceno t on f.inv_invoiceno_id = t.invno_id
    inner join tbt_invoicebodys b on f.inv_id = b.inv_body_invid_id
    inner join tbt_orderplanbodys pb on b.inv_body_orderid_id = pb.ord_body_id
    inner join tbt_orderplans p on pb.ord_body_grpordno_id = p.ord_plan_id
    inner join tbm_territories tt on p.ord_plan_custid_id = tt.terr_id
    inner join tbm_customers tc on tt.terr_customer_id = tc.cust_id
    inner join tbm_shiptypes ts on p.ora_plan_shiptype_id = ts.ship_id
    inner join tbm_customeraddress ta on tc.cust_id = ta.cust_id_id
    where t.invno_invoiceno='{invno}'
    group by tc.affcode,tc.consignee,tc.custname,ts.ship_title,ta.cust_company,p.ord_plan_etdtap,ts.ship_title
    """
    # print(sql)
    docs = __PsFetchAll(sql)
    if len(docs) > 0:
        return [docs[0][0], docs[0][1], docs[0][2], docs[0][3], docs[0][4], docs[0][5]]

    return ["", "", ""]

def appendspace(txt, ltxt):
    t = str(txt).ljust(int(ltxt))[0:int(ltxt)]
    return t

def __GetLotAndSerial(fticketno):
    sql = f"""
    SELECT c.LOTNO,d.CTNSN,d.FTICKETNO FROM TXP_ISSPACKDETAIL d
    INNER JOIN TXP_CARTONDETAILS c ON d.CTNSN = c.RUNNINGNO
    WHERE d.FTICKETNO='{fticketno}'
    """
    serialno = ""
    doc = __OraFetchAll(sql)
    i = 0
    while i < len(doc):
        lotno = str(doc[i][0])
        serialno = str(doc[i][1])
        i += 1

    return serialno

def __get_issue_ent(invno):
    sql = f"""
    select
        'AV00' recid,t.invno_invoiceno avivno,to_char(f.inv_etd,'yyyyMMdd') avivdt,'' avshpc,'' avabt,f.inv_pc avpc,f.inv_commercial avcomm,'' avvsl1,'.' avptrm,substring(f.inv_zonecode,1, 10) avzone,
        '' avshpf,'' avshpt,'.' via,'000' title
    from tbt_invoiceinfomations f
    inner join tbt_invoiceno t on f.inv_invoiceno_id = t.invno_id
    where t.invno_invoiceno='{invno}'
    """
    print(sql)
    filename =  f"{invno}{datetime.now().strftime('%Y%m%d%H%M%S')}.txt"
    gedi_file = open(f'exports/{filename}', "a")
    docs = __PsFetchAll(sql)

    avzone          =   None
    i = 0
    while i < len(docs):
        cust            =   __get_customer(invno)
        r               =   docs[i]
        recid           =   appendspace(str(r[0]), 4)
        avivno          =   appendspace(str(r[1]), 10)
        avivdt          =   appendspace(str(r[2]), 8)
        avshpc          =   appendspace(str(cust[1]), 8)
        avabt           =   appendspace(str(cust[3]), 1)
        avbltn          =   appendspace(str(cust[1]), 8)
        avpc            =   appendspace(str(r[5]), 1)
        avcomm          =   appendspace(str(r[6]), 1)
        avvsl1          =   appendspace(str(r[7]), 30)#appendspace(str(cust[4]), 30)
        avptrm          =   appendspace(str(r[8]), 2)
        # avzone          =   appendspace(str(cust[5]), 10)
        avzone          =   appendspace(str(r[1]), 10)
        avshpf          =   appendspace(str(r[10]), 15)
        avshpt          =   appendspace(str(r[11]), 15)
        via             =   appendspace(str(r[12]), 15)
        title           =   appendspace(str(r[13]), 3)
        txt             =   f"{recid}{avivno}{avivdt}{avshpc}{avabt}{avbltn}{avpc}{avcomm}{avvsl1}{avptrm}{avzone}{avshpf}{avshpt}{via}{title}"
        gedi_file.write(txt)
        i += 1

    sql_enties_detail   =   f"""
    select
        'AV01' recid,f.inv_note1 AVNT01,f.inv_note2 AVNT02,f.inv_note3 AVNT03,f.inv_ctn_total AVCTNT,'1' AVFLG2,f.user_id AVRGOP,to_char(f.inv_created_at,'yyyyMMdd') AVRGDT,to_char(f.inv_created_at,'HHMISS') AVRGTM,'' AVFGSD,f.inv_id
    from tbt_invoiceinfomations f
    inner join tbt_invoiceno t on f.inv_invoiceno_id = t.invno_id
    where t.invno_invoiceno='{invno}'
    """
    print(sql_enties_detail)
    etn_list =  __PsFetchAll(sql_enties_detail)
    i = 0
    while i < len(etn_list):
        r           =   etn_list[i]

        sql_bioat   =   f"""
        select p.ord_plan_bioabt from tbt_invoicebodys t
        inner join tbt_orderplanbodys b on t.inv_body_orderid_id = b.ord_body_id
        inner join tbt_orderplans p on b.ord_body_grpordno_id=p.ord_plan_id
        where t.inv_body_invid_id='{str(r[10])}'
        group by p.ord_plan_bioabt
        """
        bioat       =   __PsFetchOne(sql_bioat)
        recid       =   appendspace(str(r[0]), 4)
        avnt01      =   appendspace(str(r[1]), 50)
        avnt02      =   appendspace(str(r[2]), 50)
        avnt03      =   appendspace(str(r[3]), 50)
        avctnt      =   appendspace(str("{:09n}".format(int(r[4]))), 9)
        avflg2      =   appendspace(str(bioat), 1)
        avrgop      =   appendspace(f"SPL{str('{:06n}'.format(int(r[6])))}", 10)
        avrgdt      =   appendspace(str(r[7]), 8)
        avrgtm      =   appendspace(str(r[8]), 6)
        avfgsd      =   appendspace(str(r[9]), 1)
        txt         =   f"\n{recid}{avnt01}{avnt02}{avnt03}{avctnt}{avflg2}{avrgop}{avrgdt}{avrgtm}{avfgsd}"
        gedi_file.write(txt)
        i += 1

    sql_issue_body = f"""
    select
        'AW00' recid,
        t.invno_invoiceno AWIVNO,
		tc.affcode AWAC,
		to_char(p.ord_plan_etdtap,'yyMMdd')||ts.ship_title||'1' AWZONE,
		tp.part_no AWYPAT,
		pb.ord_body_orderno AWODPO,
		pb.ord_body_bistdp AWPAKQ,
		case when lp.inv_plno is null then 'PXXX' else lp.inv_plno end AWAETC,
		case when lp.ctn is null then 0 else lp.ctn end AWCTN,
		pb.ord_body_bistdp*(case when lp.ctn is null then 0 else lp.ctn end) AWSPNQ,
		ts.ship_title AWISFL,
		f.user_id AWRGOP,
		to_char(b.inv_body_created_at,'yyyyMMdd') AWRGDT,
		to_char(b.inv_body_created_at,'HHMISS') AWRGTM
    from tbt_invoiceinfomations f
    inner join tbt_invoiceno t on f.inv_invoiceno_id = t.invno_id
    inner join tbt_invoicebodys b on f.inv_id = b.inv_body_invid_id
    inner join tbt_orderplanbodys pb on b.inv_body_orderid_id = pb.ord_body_id
    inner join tbt_orderplans p on pb.ord_body_grpordno_id = p.ord_plan_id
    inner join tbm_territories tt on p.ord_plan_custid_id = tt.terr_id
    inner join tbm_customers tc on tt.terr_customer_id = tc.cust_id
    inner join tbm_shiptypes ts on p.ora_plan_shiptype_id = ts.ship_id
    inner join tbm_customeraddress ta on tc.cust_id = ta.cust_id_id
    inner join tbm_parts tp on pb.ord_body_partid_id = tp.part_id
    left join (
    	select l.inv_plno,l.inv_pl_invid_id,pp.inv_pack_bodyid_id,count(1) ctn from tbt_invoicepallets l
    	inner join tbt_invoicepackings pp on l.inv_pl_id = pp.inv_pack_plid_id
    	group by l.inv_plno,l.inv_pl_invid_id,pp.inv_pack_bodyid_id
    ) lp on f.inv_id = lp.inv_pl_invid_id and b.inv_body_id = lp.inv_pack_bodyid_id
    where t.invno_invoiceno='{invno}'
    order by lp.inv_plno,tp.part_no,pb.ord_body_orderno"""

    print(sql_issue_body)
    docs_issu_body = __PsFetchAll(sql_issue_body)

    i = 0
    while i < len(docs_issu_body):
        r                   =   docs_issu_body[i]
        recid               =   appendspace(str(r[0]), 4)
        awivno              =   appendspace(str(r[1]), 10)
        awac                =   appendspace(str(r[2]), 8)
        # awzone              =   appendspace(str(r[3]), 10)
        awzone              =   appendspace(str(r[1]), 10)
        awypat              =   appendspace(str(r[4]), 25)
        awodpo              =   appendspace(str(r[5]), 15)
        awpakq              =   appendspace(str("{:07n}".format(int(r[6]))), 7)
        awaetc              =   appendspace(str(r[7]).strip(), 12)
        awctn               =   appendspace(str("{:05n}".format(int(r[8]))), 5)
        awspnq              =   appendspace(str("{:09n}".format(int(r[9]))), 9)
        awisfl              =   appendspace(str(r[10]), 1)
        awrgop              =   appendspace(f"SPL{str('{:06n}'.format(int(r[11])))}", 10)
        awrgdt              =   appendspace(str(r[12]),8)
        awrgtm              =   appendspace(str(r[13]), 6)
        txt         =   f"\n{recid}{awivno}{awac}{awzone}{awypat}{awodpo}{awpakq}{awaetc}{awctn}{awspnq}{awisfl}{awrgop}{awrgdt}{awrgtm}"
        gedi_file.write(txt)
        i += 1

    sql_packing_dertail = f"""
    select
        'BH00' recid,
        tc.custname BHAC,
        t.invno_invoiceno BHIVNO,
        to_char(p.ord_plan_etdtap,'yyyyMMdd') BHIVDT,
        l.inv_plno BHAETC,
        tp.part_no BHYPAT,
        sc.scan_lotno BHLOT,
        0 BHCOIL,
        sc.scan_serial BHSNNO,
        case when substring(t.invno_invoiceno, 1,2) ='TI' then tp.part_no else  tp.part_desc end BHDESC,
		pb.ord_body_orderno BHODPO,
		f.user_id BHRGOP,
        to_char(b.inv_body_created_at,'yyyyMMdd') BHRGDT,
		to_char(b.inv_body_created_at,'HHMISS') BHRGTM,
        pp.inv_pack_fticketno
    from tbt_invoiceinfomations f
    inner join tbt_invoiceno t on f.inv_invoiceno_id = t.invno_id
    inner join tbt_invoicebodys b on f.inv_id = b.inv_body_invid_id
    inner join tbt_orderplanbodys pb on b.inv_body_orderid_id = pb.ord_body_id
    inner join tbt_orderplans p on pb.ord_body_grpordno_id = p.ord_plan_id
    inner join tbm_territories tt on p.ord_plan_custid_id = tt.terr_id
    inner join tbm_customers tc on tt.terr_customer_id = tc.cust_id
    inner join tbm_shiptypes ts on p.ora_plan_shiptype_id = ts.ship_id
    inner join tbm_customeraddress ta on tc.cust_id = ta.cust_id_id
    inner join tbm_parts tp on pb.ord_body_partid_id = tp.part_id
    inner join tbt_invoicepallets l on l.inv_pl_invid_id = f.inv_id
    inner join tbt_invoicepackings pp on l.inv_pl_id = pp.inv_pack_plid_id
    inner join tbt_scanreceives sc on pp.inv_pack_cartonid_id = sc.scan_id
    where t.invno_invoiceno='{invno}'
    order by l.inv_plno,tp.part_no,pb.ord_body_orderno"""

    print(sql_packing_dertail)
    docs_packing    =  __PsFetchAll(sql_packing_dertail)
    i = 0
    while i < len(docs_packing):
        r                   =   docs_packing[i]

        # l                   =   __GetLotAndSerial(str(r[14]))
        recid               =   appendspace(str(r[0]), 4)
        bhac                =   appendspace(str(r[1]), 8)
        bhivno              =   appendspace(str(r[2]), 10)
        bhivdt              =   appendspace(str(r[3]), 8)
        bhaetc              =   appendspace(str(r[4])[1:], 12)
        bhypat              =   appendspace(str(r[5]), 25)
        bhlot               =   appendspace(str(r[6]), 8)
        bhcoil              =   appendspace(str("{:03n}".format((i+1))), 3)
        bhsnno              =   appendspace(str(r[8]), 18)
        bhdesc              =   appendspace(str(r[9]), 25)
        bhodpo              =   appendspace(str(r[10]), 15)
        bhrgop              =   appendspace(f"SPL{str('{:06n}'.format(int(r[11])))}", 10)
        bhrgdt              =   appendspace(str(r[12]), 8)
        bhrgtm              =   appendspace(str(r[13]), 6)
        txt                 =   f"\n{recid}{bhac}{bhivno}{bhivdt}{bhaetc}{bhypat}{bhlot}{bhcoil}{bhsnno}{bhdesc}{bhodpo}{bhrgop}{bhrgdt}{bhrgtm}"
        gedi_file.write(txt)
        i += 1

    # sql_dimension = f"""
    # select
    #     'BR00' recid,
    #     t.invno_invoiceno BRIVNO,
	# 	0 BRLINE,
	# 	case when min(substring(l.inv_plno, 3, 4)) = 'XXX' then '001' else min(substring(l.inv_plno, 3, 4)) end plmin,
	# 	case when min(substring(l.inv_plno, 3, 4)) = 'XXX' then '001' else min(substring(l.inv_plno, 3, 4)) end plmax,
	# 	l.inv_plwidth BRWIDT,
	# 	l.inv_pllength BRLENG,
	# 	l.inv_plheight BRHIGH,
	# 	sum(p.fctn) BRQTYP,
	# 	(l.inv_plwidth*l.inv_pllength*l.inv_plheight)/1000000 BRCUBI,
	# 	'CM/P.' BRUNIT,
	# 	f.user_id BRRGOP,
	# 	to_char(l.inv_pl_created_at ,'yyyyMMdd') BRRGDT,
	# 	to_char(l.inv_pl_created_at,'HHMISS') BRRGTM
    # from tbt_invoiceinfomations f
    # inner join tbt_invoiceno t on f.inv_invoiceno_id = t.invno_id
    # inner join tbt_invoicepallets l on l.inv_pl_invid_id = f.inv_id
    # inner join (select pp.inv_pack_plid_id,count(pp.inv_pack_fticketno) fctn from tbt_invoicepackings pp group by pp.inv_pack_plid_id) p on l.inv_pl_id = p.inv_pack_plid_id
    # where t.invno_invoiceno='{invno}'
    # group by t.invno_invoiceno,l.inv_plwidth,l.inv_pllength,l.inv_plheight,f.user_id,l.inv_pl_created_at
    # order by min(l.inv_plno),max(l.inv_plno)"""

    sql_dimension = f"""
    select
        'BR00' recid,
        t.invno_invoiceno BRIVNO,
		0 BRLINE,
		case when min(substring(l.inv_plno, 3, 4)) = 'XXX' then '001' else min(substring(l.inv_plno, 3, 4)) end plmin,
		case when max(substring(l.inv_plno, 3, 4)) = 'XXX' then '001' else max(substring(l.inv_plno, 3, 4)) end plmax,
		l.inv_plwidth BRWIDT,
		l.inv_pllength BRLENG,
		l.inv_plheight BRHIGH,
		sum(p.fctn) BRQTYP,
		(l.inv_plwidth*l.inv_pllength*l.inv_plheight)/1000000 BRCUBI,
		'CM/P.' BRUNIT,
		f.user_id BRRGOP,
		to_char(min(l.inv_pl_created_at) ,'yyyyMMdd') BRRGDT,
		to_char(max(l.inv_pl_created_at),'HHMISS') BRRGTM
    from tbt_invoiceinfomations f
    inner join tbt_invoiceno t on f.inv_invoiceno_id = t.invno_id
    inner join tbt_invoicepallets l on l.inv_pl_invid_id = f.inv_id
    inner join (select pp.inv_pack_plid_id,count(pp.inv_pack_fticketno) fctn from tbt_invoicepackings pp group by pp.inv_pack_plid_id) p on l.inv_pl_id = p.inv_pack_plid_id
    where t.invno_invoiceno='{invno}'
    group by t.invno_invoiceno,l.inv_plwidth,l.inv_pllength,l.inv_plheight,f.user_id
    order by min(l.inv_plno),max(l.inv_plno)
    """

    print(sql_dimension)
    docs_dimens = __PsFetchAll(sql_dimension)
    i = 0
    while i < len(docs_dimens):
        r                =   docs_dimens[i]
        recid            =   appendspace(str(r[0]), 4)
        brivno           =   appendspace(str(r[1]), 10)
        brline           =   appendspace("{:02n}".format(i + 1), 2)
        plmin            =   int(r[3])
        plmax            =   int(r[4])
        pln              =   f"{plmin}-{plmax}"
        if plmin == plmax:
            pln          =   f"{plmin}"
        brdesc           =   appendspace(f"P/NO.{pln}", 50)
        brwidt           =   appendspace("{:04n}".format(int(r[5])), 4)
        brleng           =   appendspace("{:04n}".format(int(r[6])), 4)
        brhigh           =   appendspace("{:04n}".format(int(r[7])), 4)
        brqtyp           =   appendspace("{:03n}".format(int(r[8])), 3)
        brcubi           =   appendspace("{:05n}".format(int(r[9])), 5)
        brunit           =   appendspace(str(r[10]), 10)
        brrgop           =   appendspace(f"SPL{str('{:06n}'.format(int(r[11])))}", 10)
        brrgdt           =   appendspace(str(r[12]), 8)
        brrgtm           =   appendspace(str(r[13]), 6)
        i += 1
        txt                 =   f"\n{recid}{brivno}{brline}{brdesc}{brwidt}{brleng}{brhigh}{brqtyp}{brcubi}{brunit}{brrgop}{brrgdt}{brrgtm}"
        gedi_file.write(txt)

    gedi_file.close()
    return filename

def uploadfilegedi(filename, batchid):
        url = f"https://{os.getenv('HOST')}:{os.getenv('PORT')}/cehttp/servlet/MailboxServlet"
        passwd = urllib.parse.quote(os.getenv('PASSWORD'))
        payload = f"operation=LOGON&remote={os.getenv('USERCLOUD')}&password={passwd}"
        headers = {'Content-Type': "application/x-www-form-urlencoded"}
        urllib3.disable_warnings()
        response = requests.request("POST", url, data=payload, headers=headers, verify=False, timeout=3)
        docs = False
        if response.status_code == 200:
            try:
                f = open(f'exports/{filename}', 'r', encoding='ascii')
                files = {'filename': (f"{batchid}", f)}
                values = {
                    "operation": "UPLOAD",
                    "data_format": "A",
                    "mailbox_id": f"{os.getenv('MAIL_TO')}",
                    "batch_id": f"{batchid}",
                    "put_flag": "transmit",
                    "sourcefile": f"{files}"
                }
                r = requests.post(url, files=files, data=values,
                                  verify=False, timeout=20, cookies=response.cookies)
                soup = BeautifulSoup(r.text, 'html.parser')
                print(soup)
                docs = True
                # os.remove(f"temp/{batchid}")
                f.close()
                response.cookies = None
                response.close()

            except requests.RequestException as ex:
                print(colored(ex, "red"))
                docs = False

        return docs


def linenotifies(message):
    url = "https://notify-api.line.me/api/notify"
    payload = f"message={message}"
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Authorization': f"Bearer {os.getenv('LINE_TOKEN')}",
    }
    response = requests.request("POST", url, data=payload, headers=headers)
    return response.text

def upload_gedi(invno):
    return __get_issue_ent(invno)

def __move_to_file(source, zname):
    dirname =f"{os.path.expanduser('~/')}GEDI/CK/UPLOAD/{datetime.now().strftime('%Y%m%d')}/{zname}"
    print(f"CHECK PATH: {os.path.exists(dirname)}")
    if os.path.exists(dirname) is False:
        os.makedirs(dirname)

    destination = f"{dirname}/{source}"
    shutil.move(f"exports/{source}", destination)

def __check_last_invoice_prepare():
    sql = f"""
    select t.invno_invoiceno,f.inv_id,(l.plcount-lx.plcount) completed,bb.zone_title,to_char(f.inv_etd, 'yyyyMMdd') from tbt_invoiceno t
    inner join tbt_invoiceinfomations f on t.invno_id =f.inv_invoiceno_id
    inner join (
        select tz.zone_title,f.inv_invoiceno_id,f.inv_id,sum(tb.ord_body_balqty/b.inv_body_stdpack_qty) rcount,sum(tb.ord_body_balqty/b.inv_body_stdpack_qty)-sum(b.inv_body_set_pallet) ctn from tbt_invoicebodys b
        inner join tbt_invoiceinfomations f on b.inv_body_invid_id = f.inv_id
        inner join tbt_orderplanbodys tb on b.inv_body_orderid_id = tb.ord_body_id
        inner join tbt_orderplans o on tb.ord_body_grpordno_id = o.ord_plan_id
        inner join tbm_zones tz on o.ora_plan_zone_id = tz.zone_id
        group by tz.zone_title,f.inv_invoiceno_id,f.inv_id
    ) bb on t.invno_id = bb.inv_invoiceno_id
    inner join (
        select t.inv_pl_invid_id,count(t.inv_pl_invid_id) plcount from tbt_invoicepallets t
        inner join tbt_invoicepackings p on t.inv_pl_id=p.inv_pack_plid_id
        group by t.inv_pl_invid_id
    )l on f.inv_id = l.inv_pl_invid_id
    inner join (
        select t.inv_pl_invid_id,count(t.inv_pl_invid_id) plcount from tbt_invoicepallets t
        inner join tbt_invoicepackings p on t.inv_pl_id=p.inv_pack_plid_id
        where p.inv_pack_cartonid_id is not null
        group by t.inv_pl_invid_id
    )lx on f.inv_id = lx.inv_pl_invid_id
    where t.invno_status < 4 and (l.plcount-lx.plcount) = 0 and substring(t.invno_invoiceno,1, 2) in ('TI', 'TW')
    """
    doc = __PsFetchAll(sql)
    for i in doc:
        fname = upload_gedi(i[0])

        ## disable for develop
        # uploadfilegedi(fname, fname)

        __move_to_file(fname, f"{str(i[3])}")
        # os.remove(fname)
        msg = f"""UPLOAD GEDI\nINVOICENO.: {i[0]}\nZONE:{str(i[3])}\nBATCHID: {str(fname).replace(".txt","")}\nAT: {datetime.now().strftime('%Y-%m-%d %X')}"""
        linenotifies(msg)
        __PsExcute(f"update tbt_invoiceno set invno_status=4 where invno_invoiceno='{i[0]}'")

def __get_nesc_icam_invoice():
    sql = f"""
    select t.invno_invoiceno,bb.rcount,bb.ctn,bb.zone_title,bb.inv_id,bb.zone_title,l.plctn,(bb.rcount-l.plctn) notsetpl,to_char(bb.inv_etd, 'yyyyMMdd') from tbt_invoiceno t
    inner join (
                select f.inv_etd,tz.zone_title,f.inv_invoiceno_id,f.inv_id,sum(tb.ord_body_balqty/b.inv_body_stdpack_qty) rcount,sum(tb.ord_body_balqty/b.inv_body_stdpack_qty)-sum(b.inv_body_set_pallet) ctn from tbt_invoicebodys b
                inner join tbt_invoiceinfomations f on b.inv_body_invid_id = f.inv_id
                inner join tbt_orderplanbodys tb on b.inv_body_orderid_id = tb.ord_body_id
                inner join tbt_orderplans o on tb.ord_body_grpordno_id = o.ord_plan_id
                inner join tbm_zones tz on o.ora_plan_zone_id = tz.zone_id
                group by tz.zone_title,f.inv_invoiceno_id,f.inv_id
    ) bb on t.invno_id = bb.inv_invoiceno_id
    inner join (
        select t.inv_pl_invid_id,count(t.inv_pl_invid_id) plctn from tbt_invoicepallets t
        inner join tbt_invoicepackings p on t.inv_pl_id = p.inv_pack_plid_id
        group by t.inv_pl_invid_id
    )l on bb.inv_id = l.inv_pl_invid_id
    where t.invno_status < 4 and (bb.rcount-l.plctn) < 1 and bb.zone_title in ('NESC', 'ICAM')"""

    i = __PsFetchAll(sql)
    for j in i:
        fname = upload_gedi(j[0])
        ## disable for develop
        # uploadfilegedi(fname, fname)
        __move_to_file(fname, f"{str(j[3])}")
        # os.remove(fname)
        msg = f"""UPLOAD GEDI\nINVOICENO.: {j[0]}\nZONE: {j[3]}\nBATCHID: {str(fname).replace(".txt","")}\nAT: {datetime.now().strftime('%Y-%m-%d %X')}"""
        linenotifies(msg)
        __PsExcute(f"update tbt_invoiceno set invno_status=4 where invno_invoiceno='{j[0]}'")

if __name__ == "__main__":
    ## __get_all_invoice()
    __check_last_invoice_prepare()
    __get_nesc_icam_invoice()
    # upload_gedi("TIEM10032B")
    conn.close()
    sys.exit(0)
