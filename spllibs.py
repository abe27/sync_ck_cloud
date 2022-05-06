import urllib
import urllib3
import requests
import os
from termcolor import colored
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from shareplum import Office365
from shareplum import Site
from shareplum.site import Version

class LogActivity:
    def __init__(self, name='CK', subject=None, status='Active', message=None):
        d = datetime.now().strftime('%Y%m%d')
        filename = f"{name}-{d}.log"
        f = open(filename, 'a+')
        txt = f"{str(datetime.now().strftime('%Y%m%d %H:%M:%S')).ljust(25)}SUBJECT: {str(subject).ljust(20)}STATUS: {str(status).ljust(10)}MESSAGE: {str(message).ljust(50)}"
        f.write(f"{txt}\n")
        f.close()

class SplSharePoint:
    def __init__(self, url, site, username, password):
        self.url = url
        self.site = site
        self.username = username
        self.password = password
        
    def upload(self, pathname, filename, destination="Temp"):
        try:
            authcookie = Office365(self.url, username=self.username, password=self.password).GetCookies()
            site = Site(f'{self.url}/sites/{self.site}', version=Version.v365, authcookie=authcookie);
            folder = site.Folder(f'Shared Documents/{destination}')
            with open(pathname, mode='rb') as file:
                    fileContent = file.read()
                    
            folder.upload_file(fileContent, filename)
            LogActivity(name="SPL", subject="SHAREPOINT", status="Success", message=f"Backup GEDI({filename})")
            
        except Exception as e:
            LogActivity(name="SPL", subject="SHAREPOINT", status="Error", message=str(e))
            pass

class ObjectLink:
    def __init__(
        self,
        host,
        objtype,
        mailbox,
        batchid,
        size,
        batchfile,
        currentdate,
        flags,
        formats,
        orgname,
        download=False,
        pathname="EXPORT"
    ):
        # import os
        from datetime import datetime

        ordn = None
        bf = 0
        filetype = "RECEIVE"
        factory = "INJ"

        if objtype == "RMW":
            ordn = str(batchfile).strip()
            factory = "RMW"
            filename = ""
            if ordn[:3] == "OES":
                filename = ordn[len("OES.32TE.SPL."):]
            else:
                filename = ordn[len("NRRIS.32TE.SPL."):]

            filename = filename[: filename.find(".")].upper()
            if filename == "ISSUELIST":
                filetype = "CONLOT"

            elif filename == "ISSUENO":
                filetype = "KANBAN"

            else:
                filetype = "RECEIVE"

        elif objtype == "CK2":
            ordn = str(batchfile[: len("OES.VCBI")]).strip()
            bf = int(str(batchfile[len("OES.VCBI") + 3:])[1:2].strip())
            filetype = "RECEIVE"
            if ordn == "OES.VCBI":
                filetype = "ORDERPLAN"

            factory = "INJ"
            if bf == 4:
                factory = "AW"

        elif objtype == "J03":
            print("J03")

        elif objtype == "FG":
            print("FG")

        else:
            print("UNKNOW")

        self.objtype = objtype
        self.mailbox = mailbox
        self.batchid = batchid
        self.size = size
        self.batchfile = batchfile
        self.currentdate = datetime.strptime(currentdate, "%b %d, %Y %I:%M %p")
        self.flags = flags
        self.formats = formats
        self.orgname = orgname
        self.factory = factory
        self.filetype = filetype
        self.download = download
        self.destination = f'{pathname}/{filetype}/{(self.currentdate).strftime("%Y%m%d")}'
        self.linkfile = f"{host}/cehttp/servlet/MailboxServlet?operation=DOWNLOAD&mailbox_id={self.mailbox}&batch_num={self.batchid}&data_format=A&batch_id={self.batchfile}"


class Yazaki:
    def __init__(self, service_type="CK2", host="https://218.225.124.157:9443", username=None, password=None):
        self.service_type = service_type
        self.host = host
        self.username = username
        self.password = password

    # @staticmethod
    def login(self):
        response = False
        try:
            # login yazaki website.
            url = f"{self.host}/cehttp/servlet/MailboxServlet"
            passwd = urllib.parse.quote(self.password)
            payload = (
                f"operation=LOGON&remote={self.username}&password={passwd}"
            )
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            urllib3.disable_warnings()
            response = requests.request(
                "POST", url, headers=headers, verify=False, data=payload, timeout=3)

            txt = None
            docs = BeautifulSoup(response.text, "html.parser")
            for i in docs.find_all("hr"):
                txt = (i.previous).replace("\n", "")

            _txt_status = "Success"
            if txt.find("751") >= 0:
                _txt_status = "Error"
                response = False

            LogActivity(subject="LOGIN", status=_txt_status, message=str(txt))

        except Exception as msg:
            LogActivity(subject="LOGIN", status='Error', message=str(msg))
            pass

        return response

    def logout(self, session):
        response = True
        try:
            url = f"{self.host}/cehttp/servlet/MailboxServlet?operation=LOGOFF"
            headers = {}
            payload = {}
            rq = requests.request(
                "POST",
                url,
                data=payload,
                headers=headers,
                verify=False,
                timeout=3,
                cookies=session.cookies,
            )

            docs = BeautifulSoup(rq.text, "html.parser")
            for i in docs.find_all("hr"):
                txt = (i.previous).replace("\n", "")

            _txt_status = "Success"
            if txt.find("751") >= 0:
                _txt_status = "Error"
                response = False

            LogActivity(subject="LOGOUT", status=_txt_status, message=str(txt))

        except Exception as txt:
            LogActivity(subject="LOGOUT", status='Error', message=str(txt))
            pass

        return response

    def get_link(self, session):
        obj = []
        try:
            etd = str((datetime.now() -
                      timedelta(days=1)).strftime("%Y%m%d"))
            # etd = '20220505'
            # get cookies after login.
            if session.status_code == 200:
                # get html page
                url = f"{self.host}/cehttp/servlet/MailboxServlet"
                headers = {"Content-Type": "application/x-www-form-urlencoded"}
                payload = f"operation=DIRECTORY&fromdate={etd}&Submit=Receive"
                r = requests.request(
                    "POST",
                    url,
                    data=payload,
                    headers=headers,
                    verify=False,
                    timeout=3,
                    cookies=session.cookies,
                )
                # print(type(r))
                soup = BeautifulSoup(r.text, "html.parser")
                for tr in soup.find_all("tr"):
                    found = False
                    i = 0
                    docs = []
                    for td in tr.find_all("td"):
                        txt = (td.text).rstrip().lstrip()
                        docs.append(txt)
                        if td.find("a") != None:
                            found = True

                        if found is True:  # False =debug,True=prod.
                            if len(docs) >= 9:
                                l = ObjectLink(
                                    self.host,
                                    self.service_type,
                                    docs[0],
                                    docs[1],
                                    str(docs[2]).replace(",", "").strip(),
                                    docs[3],
                                    f"{docs[4]} {docs[5]}",
                                    docs[6],
                                    docs[7],
                                    docs[8],
                                    found,
                                )
                                obj.append(l)

                        i += 1

                print(colored(f"found new link => {len(obj)}", "green"))
                LogActivity(subject="GET LINK", status='Success',
                            message=f"FOUND NEW LINK({len(obj)})")

        except Exception as ex:
            LogActivity(subject="GET LINK", status='Error', message=str(ex))
            pass

        return obj

    def download_gedi_files(self, session, obj):
        filename = f"{obj.destination}/{obj.batchid}.{obj.batchfile}"
        try:
            # print(obj)
            # makedir folder gedi is exits
            os.makedirs(obj.destination, exist_ok=True)
            # download file
            request = requests.get(
                obj.linkfile,
                stream=True,
                verify=False,
                cookies=session.cookies,
                allow_redirects=True,
            )
            docs = BeautifulSoup(request.content, "lxml")

            # Write data to GEDI File
            f = open(filename, mode="a", encoding="ascii", newline="\r\n")
            for p in docs:
                f.write(p.text)
            f.close()

            LogActivity(subject="DOWNLOAD", status='Success',
                        message=f"Download GEDI FILE({obj.batchfile})")

        except Exception as ex:
            LogActivity(subject="DOWNLOAD", status='Error', message=str(ex))
            filename = None
            pass
        return filename


class SplApi:
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = urllib.parse.quote(password)

    def login(self):
        try:
            url = f"{self.host}/login"
            payload = f'empcode={self.username}&password={self.password}'
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            response = requests.request("POST", url, headers=headers, data=payload)
            if response.status_code == 200:
                obj = response.json()
                LogActivity(name='SPL',subject="LOGIN", status='Success', message=f"Token is {obj['access_token']}")
                return obj['access_token']
            
        except Exception as ex:
            LogActivity(name='SPL',subject="LOGIN", status='Error', message=str(ex))
            pass

        return None

    def logout(self, token):
        try:
            url = f"{self.host}/logout"
            payload = {}
            headers = {
                'Authorization': f'Bearer {token}'
            }

            response = requests.request("GET", url, headers=headers, data=payload)

            if response.status_code == 200:
                LogActivity(name='SPL',subject="LOGOUT", status='Success', message=f"Logoff By {token}")
                return True
            
        except Exception as ex:
            LogActivity(name='SPL',subject="LOGOUT", status='Error', message=str(ex))
            pass

        return False
    
    def upload(self, whsId, typeName, batchId, filepath, filename, token):
        try:
            url = f"{self.host}/gedi/store"
            payload={
                'whs_id': whsId,
                'file_type': typeName,
                'batch_id': batchId
            }
            files=[
                ('file_name',(filename,open(filepath,'rb'),'application/octet-stream'))
            ]
            headers = {
                'Authorization': f'Bearer {token}'
            }

            response = requests.request("POST", url, headers=headers, data=payload, files=files)
            if response.status_code == 200:
                LogActivity(name='SPL', subject="UPLOAD", status='Success',message=f"Upload GEDI({filename})")
                return True
            
        except Exception as ex:
            LogActivity(name='SPL', subject="UPLOAD", status='Error',message=str(ex))
            pass
        
        return False
    