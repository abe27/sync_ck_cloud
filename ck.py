import sys
import os
from datetime import datetime
from spllibs import Yazaki, SplApi, SplSharePoint, LogActivity as log
from bs4 import BeautifulSoup
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
            ### download gedi file
            yk.download_gedi_files(session, x)
            
            print(f"download gedi file => {x.batchfile}")   
            i += 1
            
        ### logout
        yk.logout(session)
        
    ### Stop service
    log(subject="STOP", status='Active',message=f"SERVICE IS EXITED")
    
    
    ### Upload gedi to SPL Server
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
                        batchId = str(name[:8]).replace('.', '')
                        filepath = os.path.join(root_pathname, x, p, name)
                        print(f"Date: {p}")
                        print(f"WHS: CK-2")
                        print(f"Batch ID: {batchId}")
                        print(f"File Name: {name[8:]}")
                        print(f"TYPE: {x[:1]}")
                        print(f"Path: {filepath}")
                        print('-------------------------------------------\n')
                        
                        ### upload file to SPL Server
                        spl.upload("CK-2", x[:1], batchId, filepath, name[8:], spl_token)
                        ### Upload file to SPL Share Point
                        share_file.upload(filepath, name[8:], f'GEDI/{x}/{p}')
            
            is_success = spl.logout(spl_token)
            print(f'logout is {is_success}')
            
        # i = 0
        # while i < len(file_for_upload):
        #     r = file_for_upload[i]
            
        #     i += 1
        log(name='SPL', subject="STOP", status='Active',message=f"Stop SPL Service")
        
    ### Delete EXPORT Folder
    log(name='SPL', subject="DELETE", status='Active',message=f"Delete EXPORT Folder")
    
if __name__ == '__main__':
    main()
    sys.exit(0)