import pandas as pd
import psycopg2 as pgsql
import cx_Oracle
import os
from nanoid import generate

DB_HOSTNAME=os.environ.get('DATABASE_URL')
DB_PORT=os.environ.get('DATABASE_PORT')
DB_NAME=os.environ.get('DATABASE_NAME')
DB_USERNAME=os.environ.get('DATABASE_USERNAME')
DB_PASSWORD=os.environ.get('DATABASE_PASSWORD')

def main():
    Oracon = cx_Oracle.connect(user="expsys",password="expsys",dsn="192.168.101.215/RMW")
    Oracur = Oracon.cursor()
    
    mydb = pgsql.connect(
        host=DB_HOSTNAME,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    
    # data = pd.read_excel('./Data/press.xlsx')
    # obj = data.get('part_no')
    # rnd = 1
    # for i in obj:
    #     sql = Oracur.execute(f"SELECT partno FROM txp_part c WHERE c.partno='{i}'")
    #     sql_insert_part = f""
    #     if sql:
    #         sql_insert_part = f"update txp_part set type='PRESS' where partno='{i}'"
            
    #     Oracur.execute(sql_insert_part)
    #     print(f"{rnd}. {i}")
    #     rnd += 1
    mycursor = mydb.cursor()
    
    part = Oracur.execute(f"SELECT PARTNO,CASE WHEN TYPE IS NULL THEN '-' ELSE CASE WHEN TYPE = 'PRESS' THEN 'PLATE' ELSE TYPE END  END part_type,PARTNAME  FROM TXP_PART")
    for i in part.fetchall():
        part_no = str(i[0])
        part_name = str(i[2])
        part_type = str(i[1])
        
        mycursor.execute(f"select id from tbt_parts where no='{part_no}'")
        p = mycursor.fetchone()
        if p == None:
            sql_part = f"insert into tbt_parts(id,no,name,is_active,created_at,updated_at) values ('{generate(size=36)}','{part_no}','{part_name}',true,current_timestamp, current_timestamp)"
            mycursor.execute(sql_part)

        mycursor.execute(f"select id from tbt_part_types where name='{part_type}'")
        type_id = mycursor.fetchone()[0]
        
        mycursor.execute(f"update tbt_ledgers set part_type_id='{type_id}' where part_id='{p[0]}'")
        print(f"{p[0]} ==> {part_no} type id: {type_id}")
        
    Oracon.commit()
    Oracon.close()
    
    mydb.commit()
    mydb.close()
    
if __name__ == '__main__':
    main()
    