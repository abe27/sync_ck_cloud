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
        
def create_invoice():
    sql = f"select substring(bhivno, 6, 4) inv,bhivno,order_plan_id from tbt_invoice_checks where order_plan_id is not null order by bhivdt,bhivno,bhpaln,bhodpo,bhypat"
    pg_cursor.execute(sql)
    db = pg_cursor.fetchall()
    
    inv_check = None
    for i in db:
        runn_no = int(str(i[0]))
        invoice_no = str(i[1]).strip()
        order_plan_id = str(i[2]).strip()
        
        if inv_check is None:
            inv_check = invoice_no
            
        if inv_check != invoice_no:
            inv_check = invoice_no
        
    print(f"-->")

if __name__ == '__main__':
    # main()
    create_invoice()
    pgdb.commit()
    pgdb.close()
    sys.exit(0)