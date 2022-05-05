import urllib
import urllib3
import requests
import os
import datetime
from datetime import timedelta
from termcolor import colored
from bs4 import BeautifulSoup
from log import LogActivity as log

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
                filename = ordn[len("OES.32TE.SPL.") :]
            else:
                filename = ordn[len("NRRIS.32TE.SPL.") :]

            filename = filename[: filename.find(".")].upper()
            if filename == "ISSUELIST":
                filetype = "CONLOT"

            elif filename == "ISSUENO":
                filetype = "KANBAN"

            else:
                filetype = "RECEIVE"

        elif objtype == "CK2":
            ordn = str(batchfile[: len("OES.VCBI")]).strip()
            bf = int(str(batchfile[len("OES.VCBI") + 3 :])[1:2].strip())
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
            response = requests.request("POST", url, headers=headers, verify=False, data=payload, timeout=3)

            txt = None
            docs = BeautifulSoup(response.text, "html.parser")
            for i in docs.find_all("hr"):
                txt = (i.previous).replace("\n", "")

            _txt_status = "Success"
            if txt.find("751") >= 0:
                _txt_status = "Error"
                response = False

            log(subject="LOGIN", status=_txt_status,message=str(txt))

        except Exception as msg:
            log(subject="LOGIN", status='Error',message=str(msg))
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
                
            log(subject="LOGOUT", status=_txt_status,message=str(txt))
            
        except Exception as txt:
            log(subject="LOGOUT", status='Error',message=str(txt))
            pass

        return response
    
    def get_link(self, session):
        obj = []
        try:
            etd = str((datetime.datetime.now() - timedelta(days=1)).strftime("%Y%m%d"))
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

                        if found is False:  ### False =debug,True=prod.
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
                log(subject="GET LINK", status='Success',message=f"FOUND NEW LINK({len(obj)})")

        except Exception as ex:
            log(subject="GET LINK", status='Error',message=str(ex))
            pass

        return obj
    
    def download_gedi_files(self, session, obj):
        filename = f"{obj.destination}/{obj.batchfile}"
        try:
            # print(obj)
            ### makedir folder gedi is exits
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
            
            ## Write data to GEDI File
            f = open(filename, mode="a", encoding="ascii", newline="\r\n")
            for p in docs:
                f.write(p.text)
            f.close()
            
            log(subject="DOWNLOAD", status='Success',message=f"Download GEDI FILE({obj.batchfile})")
            
        except Exception as ex:
            log(subject="DOWNLOAD", status='Error',message=str(ex))
            filename = None
            pass
        return filename