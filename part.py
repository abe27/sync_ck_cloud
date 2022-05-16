import pandas as pd
import cx_Oracle
import os

def main():
    Oracon = cx_Oracle.connect(user="expsys",password="expsys",dsn="192.168.101.215/RMW")
    Oracur = Oracon.cursor()
    data = pd.read_excel('./Data/carton.xlsx')
    
    for row in data:
        print(row)
        
    Oracon.commit()
    Oracon.close()
    
if __name__ == '__main__':
    main()
    