import pandas as pd
import cx_Oracle
import os

def main():
    Oracon = cx_Oracle.connect(user="expsys",password="expsys",dsn="192.168.101.215/RMW")
    Oracur = Oracon.cursor()
    part_sql = Oracur.execute(f"select partno from txp_part")
    data = part_sql.fetchall()
    for p in data:
        txt = "UPDATE"
        partno = p[0]
        part_sql = Oracur.execute(f"select partno from TXP_LEDGER where partno='{partno}'")
        if part_sql.fetchone() is None:
            txt = "INSERT"
            Oracur.execute(f"""INSERT INTO TXP_LEDGER(PARTNO,TAGRP,MINIMUM,MAXIMUM,WHS,PICSHELFBIN,STKSHELFBIN,OVSSHELFBIN,OUTERPCS,UPDDTE, SYSDTE)VALUES('{partno}', 'C',0,0,'INJ','PNON', 'SNON','ONON',0, sysdate, sysdate)""")

        print(f"{txt} check part: {partno}")
    
    Oracon.commit()
    Oracon.close()
    
if __name__ == '__main__':
    main()
    