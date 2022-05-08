import pandas as pd
import cx_Oracle
import os
from spllibs import SplApi

spl = SplApi()
def main():
    Oracon = cx_Oracle.connect(user="expsys",password="expsys",dsn="192.168.101.215/RMW")
    Oracur = Oracon.cursor()
    root_pathname = "EXPORT.BNK"
    if os.path.exists(root_pathname):
        root_path = os.listdir(root_pathname)
        for x in root_path:
            xpath = os.listdir(f'{root_pathname}/{x}')
            for p in xpath:
                pname = os.path.join(root_pathname, x, p)
                for name in os.listdir(pname):
                    fname = os.path.join(root_pathname, x, p, name)
                    if x == "ORDERPLAN":
                        head = spl.header_orderplan(name)
                        # if head['factory'] == 'INJ':
                        f = open(fname, 'r')
                        for w in f:
                            data = spl.read_orderplan(head, w)
                            part_sql = Oracur.execute(f"select partno from txp_part where partno='{data['partno']}'")
                            if part_sql.fetchone() is None:
                                Oracur.execute(f"""insert into txp_part (tagrp,partno,partname,CD,VENDORCD,UNIT ,upddte,sysdte)values('C','{data['partno']}','{data['partname']}','{data['cd']}', '{data['factory']}', '{data['unit']}',sysdate,sysdate)""")
                            
                            part_sql = Oracur.execute(f"select partno from TXP_LEDGER where partno='{data['partno']}'")
                            if part_sql.fetchone() is None:
                                Oracur.execute(f"""INSERT INTO TXP_LEDGER(PARTNO,TAGRP,MINIMUM,MAXIMUM,WHS,PICSHELFBIN,STKSHELFBIN,OVSSHELFBIN,OUTERPCS,UPDDTE, SYSDTE)VALUES('{data['partno']}', 'C',0,0,'{data['factory']}','PNON', 'SNON','ONON',0, sysdate, sysdate)""")
                            print(f"{p} check part: {data['partno']}")
                            
                        f.close()
                    else:
                        head = spl.header_receive(name)
                        # if head['factory'] == 'INJ':
                        f = open(fname, 'r')
                        for w in f:
                            data = spl.read_receive(head, w)
                            part_sql = Oracur.execute(f"select partno from txp_part where partno='{data['partno']}'")
                            if part_sql.fetchone() is None:
                                Oracur.execute(f"""insert into txp_part (tagrp,partno,partname,CD,VENDORCD,UNIT ,upddte,sysdte)values('C','{data['partno']}','{data['partname']}','{data['cd']}', '{data['factory']}', '{data['unit']}',sysdate,sysdate)""")
                            
                            part_sql = Oracur.execute(f"select partno from TXP_LEDGER where partno='{data['partno']}'")
                            if part_sql.fetchone() is None:
                                Oracur.execute(f"""INSERT INTO TXP_LEDGER(PARTNO,TAGRP,MINIMUM,MAXIMUM,WHS,PICSHELFBIN,STKSHELFBIN,OVSSHELFBIN,OUTERPCS,UPDDTE, SYSDTE)VALUES('{data['partno']}', 'C',0,0,'{data['factory']}','PNON', 'SNON','ONON',0, sysdate, sysdate)""")
                            print(f"{p} check part: {data['partno']}")
                        f.close()
                        
                    print(fname)
                    
    # Oracon = cx_Oracle.connect(user="expsys",password="expsys",dsn="192.168.101.215/RMW")
    # Oracur = Oracon.cursor()
    # data = pd.read_excel("Data/data.xlsx", sheet_name='Sheet1')
    # for i in data['part']:
    #     print(i)
    #     part_sql = Oracur.execute(f"select partno from txp_part where partno='{i}'")
    #     if part_sql.fetchone() is None:
    #         Oracur.execute(f"""insert into txp_part (tagrp,partno,partname,CD,VENDORCD,UNIT ,upddte,sysdte)values('C','{i}','{i}','20', 'INJ', 'BOX',sysdate,sysdate)""")
    #     else:
    #         Oracur.execute(f"""update txp_part set  partname='{i}',upddte=sysdate where partno='{i}'""")
            
    #     part_sql = Oracur.execute(f"select partno from TXP_LEDGER where partno='{i}'")
    #     if part_sql.fetchone() is None:
    #         Oracur.execute(f"""INSERT INTO TXP_LEDGER(PARTNO,TAGRP,MINIMUM,MAXIMUM,WHS,PICSHELFBIN,STKSHELFBIN,OVSSHELFBIN,OUTERPCS,UPDDTE, SYSDTE)VALUES('{i}', 'C',0,0,'INJ','PNON', 'SNON','ONON',0, sysdate, sysdate)""")
        
    #     else:
    #         Oracur.execute(f"""UPDATE TXP_LEDGER SET RECORDMAX=1,LASTRECDTE=sysdate,LASTISSDTE=sysdate WHERE PARTNO='{i}'""")
            
    Oracon.commit()
    Oracon.close()
    
if __name__ == '__main__':
    main()
    