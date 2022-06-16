import sys
import os
import pandas as pd
import psycopg2 as pgsql
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

def main():
    try:
        df = pd.read_excel(f"./Data/aff.xlsx", index_col=None)  
        #NoAffNameCustomerAddress
        cities = df.to_dict('records')
        for i in cities:
            aff_code = i["Aff"]
            cust_name = i['Name']
            cust_address = str(i['Customer']).strip().replace("'", "''")
            address = str(i['Addres']).strip().replace("'", "''")
            
            sql_address = f"select id from tbt_customer_addresses where address='{cust_address}'"
            pg_cursor.execute(sql_address)
            address_id = pg_cursor.fetchone()
            address_generate_id = generate(size=36)
            if address_id is None:
                sql_insert_address = f"insert into tbt_customer_addresses(id,address,description,is_active,created_at,updated_at)values('{address_generate_id}','{cust_address}','{address}',true,current_timestamp,current_timestamp)"
                pg_cursor.execute(sql_insert_address)
                
            else:
                address_generate_id = address_id[0]
            
            sql_aff = f"select id from tbt_customers where cust_code='{aff_code}' and cust_name='{cust_name}'"
            pg_cursor.execute(sql_aff)
            aff = pg_cursor.fetchone()
            if aff != None:
                aff_id = str(aff[0]).strip()
                pg_cursor.execute(f"select id from tbt_factory_types where name='INJ'")
                fac_id = pg_cursor.fetchone()[0]
                print(f"{fac_id} ==> {aff_id}")
                pg_cursor.execute(f"update tbt_consignees set address_id='{address_generate_id}' where factory_id='{fac_id}' and customer_id='{aff_id}'")
            
            
    except Exception as e:
        print(str(e))

if __name__ == '__main__':
    main()
    pgdb.commit()
    pgdb.close()
    sys.exit(0)