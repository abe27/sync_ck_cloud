import sys
import os
import time
import mysql.connector
from nanoid import generate
from spllibs import Yazaki, SplApi, SplSharePoint, LogActivity as log
from dotenv import load_dotenv
load_dotenv()

SERVICE_TYPE="CK2"
YAZAKI_HOST=f"https://{os.environ.get('HOST')}:{os.environ.get('PORT')}"
YAZAKI_USER=os.environ.get('CK_USERNAME')
YAZAKI_PASSWORD=os.environ.get('CK_PASSWORD')

SPL_API_HOST=os.environ.get('SPL_SITE_URL')
SPL_API_USERNAME=os.environ.get('SPL_USERNAME')
SPL_API_PASSWORD=os.environ.get('SPL_PASSWORD')

SHAREPOINT_SITE_URL=os.environ.get('SHAREPOINT_URL')
SHAREPOINT_SITE_NAME=os.environ.get('SHAREPOINT_URL_SITE')
SHAREPOINT_USERNAME=os.environ.get('SHAREPOINT_USERNAME')
SHAREPOINT_PASSWORD=os.environ.get('SHAREPOINT_PASSWORD')

DB_HOSTNAME=os.environ.get('DATABASE_URL')
DB_PORT=os.environ.get('DATABASE_PORT')
DB_NAME=os.environ.get('DATABASE_NAME')
DB_USERNAME=os.environ.get('DATABASE_USERNAME')
DB_PASSWORD=os.environ.get('DATABASE_PASSWORD')

### Initail Data
yk = Yazaki(SERVICE_TYPE,YAZAKI_HOST, YAZAKI_USER, YAZAKI_PASSWORD)
spl = SplApi(SPL_API_HOST, SPL_API_USERNAME, SPL_API_PASSWORD)
share_file = SplSharePoint(SHAREPOINT_SITE_URL, SHAREPOINT_SITE_NAME, SHAREPOINT_USERNAME, SHAREPOINT_PASSWORD)

def main():
    msg = f"Starting Sync CK on {YAZAKI_HOST}"
    log(subject="START", status='Active',message=msg)
    ### login
    session = yk.login()

    if session != False:
        ### get link gedi
        link = yk.get_link(session)
        i = 0
        while i < len(link):
            x = link[i]
            # ### download gedi file
            yk.download_gedi_files(session, x)
            
            print(f"download gedi file => {x.batchfile}")   
            i += 1
            
        ### logout
        yk.logout(session)
        
    ### Stop service
    log(subject="STOP", status='Active',message=f"SERVICE IS EXITED")
    
    
    ### Upload gedi to SPL Server
    batch_running = 1
    root_pathname = "EXPORT"
    # print(os.path.exists(root_pathname))
    if os.path.exists(root_pathname):
        log(name="SPL", subject="START", status='Active',message=f"Start SPL Service")
        spl_token = spl.login()
        if spl_token:
            ### Check Folder
            root_path = os.listdir(root_pathname)
            for x in root_path:
                xpath = os.listdir(f'{root_pathname}/{x}')
                for p in xpath:
                    pname = os.path.join(root_pathname, x, p)
                    for name in os.listdir(pname):
                        batchId = "{:08}".format(batch_running)
                        gedi_name = name
                        if name[:1] != "O":
                            batchId = str(name[:8]).replace('.', '')
                            gedi_name = name[8:]
                            
                        filepath = os.path.join(root_pathname, x, p, name)
                        print(f"Date: {p}")
                        print(f"WHS: CK-2")
                        print(f"Batch ID: {batchId}")
                        print(f"File Name: {gedi_name}")
                        print(f"TYPE: {x[:1]}")
                        print(f"Path: {filepath}")
                        ### upload file to SPL Server
                        is_upload = spl.upload("CK-2", x[:1], batchId, filepath, gedi_name, spl_token)
                        # ### Upload file to SPL Share Point
                        # share_file.upload(filepath, gedi_name, f'GEDI/{x}/{p}')
                        print(f"STATUS: {is_upload}")
                        batch_running += 1
                        print('-------------------------------------------\n')
                        time.sleep(3)
            
            is_success = spl.logout(spl_token)
            print(f'logout is {is_success}')

        log(name='SPL', subject="STOP", status='Active',message=f"Stop SPL Service")
        
    ### Delete EXPORT Folder
    # os.removedirs(root_pathname)
    log(name='SPL', subject="DELETE", status='Active',message=f"Delete EXPORT Folder")
    
def download():
    token = spl.login()
    obj = spl.get_link(token)
    i = 0
    while i < len(obj):
        r = obj[i]
        filename = spl.get_file(r['file_name'], r['file_path'], r['file_type'])
        if filename:
            if r['file_type'] == 'O':
                head = spl.header_orderplan(filename)
                data = []
                f = open(filename, 'r')
                for i in f:
                    b = spl.read_orderplan(head, i)
                    data.append(b)
                f.close()
                # ### remove temp files after load data.
                # os.remove(filename)
                
                ### Insert MySQL
                mydb = mysql.connector.connect(
                    host=DB_HOSTNAME,
                    port=DB_PORT,
                    user=DB_USERNAME,
                    password=DB_PASSWORD,
                    database=DB_NAME,
                )

                mycursor = mydb.cursor()
                sql = "INSERT INTO tbt_order_plans(id, file_gedi_id, vendor, cd, unit, whs, tagrp, factory, sortg1, sortg2, sortg3, plantype, pono, biac, shiptype, etdtap, partno, partname, pc, commercial, sampleflg, orderorgi, orderround, firmflg, shippedflg, shippedqty, ordermonth, balqty, bidrfl, deleteflg, ordertype, reasoncd, upddte, updtime, carriercode, bioabt, bicomd, bistdp, binewt, bigrwt, bishpc, biivpx, bisafn, biwidt, bihigh, bileng, lotno, is_active, created_at, updated_at)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                val = []
                for a in data:
                    # print(a)
                    id = generate(size=36)
                    val.append(id, r['id'], a['vendor'], a['cd'], a['unit'], a['whs'], a['tagrp'], a['factory'], a['sortg1'], a['sortg2'], a['sortg3'], a['plantype'], a['pono'], a['biac'], a['shiptype'], a['etdtap'], a['partno'], a['partname'], a['pc'], a['commercial'], a['sampleflg'], a['orderorgi'], a['orderround'], a['firmflg'], a['shippedflg'], a['shippedqty'], a['ordermonth'], a['balqty'], a['bidrfl'], a['deleteflg'], a['ordertype'], a['reasoncd'], a['upddte'], a['updtime'], a['carriercode'], a['bioabt'], a['bicomd'], a['bistdp'], a['binewt'], a['bigrwt'], a['bishpc'], a['biivpx'], a['bisafn'], a['biwidt'], a['bihigh'], a['bileng'],a['lotno'],'1', 'current_timestamp', 'current_timestamp')
                    
                ### Commit MySQL
                mycursor.execute(sql, val)
                mydb.commit()
                mydb.close()
        i += 1
    if spl.logout(token):
        print(f'end service')
    
if __name__ == '__main__':
    # main()
    # time.sleep(10)
    download()
    sys.exit(0)