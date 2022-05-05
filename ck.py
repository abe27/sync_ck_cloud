import sys
import os
import urllib
import urllib3
import requests
import time
from datetime import datetime
from log import LogActivity as log
from yazaki import Yazaki, ObjectLink
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv()

SERVICE_TYPE="CK2"
YAZAKI_HOST=f"https://{os.environ.get('HOST')}:{os.environ.get('PORT')}"
YAZAKI_USER=os.environ.get('CK_USERNAME')
YAZAKI_PASSWORD=os.environ.get('CK_PASSWORD')

yk = Yazaki(SERVICE_TYPE,YAZAKI_HOST, YAZAKI_USER, YAZAKI_PASSWORD)
def main():
    msg = f"Starting Sync CK on {YAZAKI_HOST}"
    log(subject="START", status='Active',message=msg)
    ### login
    session = yk.login()

    file_for_upload = []
    if session != False:
        ### get link gedi
        link = yk.get_link(session)
        i = 0
        while i < len(link):
            x = link[i]
            print(f"download gedi file => {x.batchfile}")
            ### download gedi file
            filename = yk.download_gedi_files(session, x)
            if filename:
                ### Append data
                file_for_upload.append(x)
                
            i += 1
            
        ### logout
        yk.logout(session)
        
    ### upload gedi to SPL Server
    log(subject="STOP", status='Active',message=f"SERVICE IS EXITED")
    
    
    
if __name__ == '__main__':
    main()
    sys.exit(0)