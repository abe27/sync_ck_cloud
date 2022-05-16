import pandas as pd
import cx_Oracle
import os

def main():
    Oracon = cx_Oracle.connect(user="expsys",password="expsys",dsn="192.168.101.215/RMW")
    Oracur = Oracon.cursor()
    data = pd.read_excel('./Data/carton.xlsx')
    obj = data.get('SERIALNO_CARTON')
    rnd = 1
    for i in obj:
        sql = Oracur.execute(f"SELECT RUNNINGNO FROM TXP_CARTONDETAILS c WHERE c.RUNNINGNO='{i}'")
        print(f"{rnd}. {sql.fetchone()[0]}")
        rnd += 1
        
        
    Oracon.commit()
    Oracon.close()
    
if __name__ == '__main__':
    main()
    