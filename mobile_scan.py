import sys
import os
import psycopg2 as pgsql
from nanoid import generate
from spllibs import LogActivity as log
from dotenv import load_dotenv
load_dotenv()

DB_HOSTNAME=os.environ.get('DATABASE_URL')
DB_PORT=os.environ.get('DATABASE_PORT')
DB_NAME=os.environ.get('DATABASE_NAME')
DB_USERNAME=os.environ.get('DATABASE_USERNAME')
DB_PASSWORD=os.environ.get('DATABASE_PASSWORD')

def main():
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    mycursor = mydb.cursor()
    
    try:
        mycursor.execute("select count(emp_id),emp_id from tbt_serial_no_triggers group by emp_id")
        for x in mycursor.fetchall():
            mycursor.execute(f"insert into tbt_mobile_scan_histories(id, on_date, scan_counter, emp_id, created_at, updated_at)values('{generate(size=36)}', current_timestamp, '{str(x[0])}', '{str(x[1])}', current_timestamp, current_timestamp)")
        
        mycursor.execute(f"truncate tbt_serial_no_triggers")
        mydb.commit()
        
    except Exception as ex:
        log(name='CARTON-ERROR', subject="UPLOAD RECEIVE", status="Error", message=str(ex))
        pass
    
    mydb.close()
    
def truncate_db():
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"truncate tbt_personal_access_tokens")
    mycursor.execute(f"delete from tbt_log_activities where created_at <= date_trunc('week', (current_date - 7))::date")
    mydb.close()

if __name__ == '__main__':
    main()
    sys.exit(0)