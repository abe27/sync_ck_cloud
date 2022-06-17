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
        df = pd.read_excel(f"./Invoice/20-24-06-2022.xls", index_col=None)  
        #NoAffNameCustomerAddress
        data = df.to_dict('records')
        for i in data:
            'BHIVNO'
            'BHODPO'
            'BHIVDT'
            'BHCONN'
            'BHCONS'
            'BHSVEN'
            'BHSHPF'
            'BHSAFN'
            'BHSHPT'
            'BHFRTN'
            'BHCON'
            'BHPALN'
            'BHPNAM'
            'BHYPAT'
            'BHCTN'
            'BHWIDT'
            'BHLENG'
            'BHHIGH'
            'BHGRWT'
            'BHCBMT'
            
            
    except Exception as e:
        print(str(e))

if __name__ == '__main__':
    main()
    pgdb.commit()
    pgdb.close()
    sys.exit(0)