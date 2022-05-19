import json
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
        path = "LOGS"
        if os.path.exists(path) is False:os.mkdir(path)
        filename = f"{path}/{name}-{d}.log"
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
            etd = str((datetime.now() - timedelta(days=1)).strftime("%Y%m%d"))
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
    
    def spec_download_gedi_files(self, session, obj):
        filename = f"{obj['destination']}/{obj['batchid']}.{obj['batchfile']}"
        try:
            # print(obj)
            # makedir folder gedi is exits
            os.makedirs(obj['destination'], exist_ok=True)
            # download file
            request = requests.get(
                obj['linkfile'],
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
                        message=f"Download GEDI FILE({obj['batchfile']})")

        except Exception as ex:
            LogActivity(subject="DOWNLOAD", status='Error', message=str(ex))
            filename = None
            pass
        return filename

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
    def __init__(self, host="http://localhost:8080", username="admin", password="admin@spl"):
        self.host = host
        self.username = username
        self.password = urllib.parse.quote(password)
        
    def __trim_txt(self, txt):
        return str(txt).lstrip().rstrip()
    
    def __check_partname(self, fac, part):
        p = str(part).lstrip().rstrip().replace(".", "")
        partname = p
        if fac == "AW":
            try:
                k = str(p[: p.index(" ")]).strip()
                s = p[len(k) :]
                ss = s.strip()
                sn = str(ss[: ss.index(" ")]).strip()
                ssize = str(ss[: ss.index(" ")])

                if len(sn) > 1:
                    ssize = str(f"{sn[:1]}.{sn[1:]}").strip()

                c = str(p[(len(k) + len(ssize)) + 1 :]).strip()
                partname = f"{k} {ssize} {c}"
            except:
                pass
            finally:
                pass

        return partname

    def __re_partname(self, txt):
        return (str(txt).replace("b", "")).replace("'", "")

    def __pono(self, txt):
        return str(self.__re_partname(txt)).strip()
    
    def line_notification(self, msg):
        url = "https://notify-api.line.me/api/notify"
        payload = f"message={msg}"
        headers = {
            "Authorization": f"Bearer {os.getenv('LINE_NOTIFICATION_TOKEN')}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        # BugDWScwhYvjVc5EyRi5sa28LmJxE2G5NIJsrs6vEV7

        response = requests.request(
            "POST", url, headers=headers, data=payload.encode("utf-8")
        )

        print(f"line status => {response}")
        if response.status_code == 200:return True
        return False

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
                'filename': filename,
                'whs_id': whsId,
                'file_type': typeName,
                'batch_id': batchId
            }
            files=[
                ('file',(filename,open(filepath,'rb'),'application/octet-stream'))
            ]
            headers = {
                'Authorization': f'Bearer {token}'
            }

            response = requests.request("POST", url, headers=headers, data=payload, files=files)
            # print(response.text)
            if response.status_code == 200:
                LogActivity(name='SPL', subject="UPLOAD", status='Success',message=f"Upload GEDI({filename})")
                return True
            
        except Exception as ex:
            LogActivity(name='SPL', subject="UPLOAD", status='Error',message=str(ex))
            pass
        
        return False
    
    def get_link(self, token, status=0):
        url = f"{self.host}/gedi/get/{status}"
        payload={}
        headers = {
            'Authorization': f'Bearer {token}'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code == 200:
            obj = response.json()
            return obj['data']
        
        return False
    
    def header_receive(self, fileNamme):
        fac = fileNamme[fileNamme.find("SPL") - 2 : fileNamme.find("SPL") - 1]
        plantype = "RECEIVE"
        cd = 20
        unit = "BOX"
        recisstype = "01"
        factory = "INJ"
        if fac != "5":
            factory = "AW"
            plantype = "RECEIVE"
            cd = 10
            unit = "COIL"
            recisstype = "01"
            
        return {
            "plantype": plantype,
            "cd": cd,
            "unit": unit,
            "recisstype": recisstype,
            "factory": factory,
        }
    
    def read_receive(self, obj, line):
        cd = obj['cd']
        unit = obj['unit']
        recisstype = obj['recisstype']
        plantype = obj['plantype']
        factory = obj['factory']
        return {
                    "factory": factory,
                    "faczone": str(line[4 : (4 + 3)]).lstrip().rstrip(),
                    "receivingkey": str(line[4 : (4 + 12)]).lstrip().rstrip(),
                    "partno": str(line[76 : (76 + 25)]).lstrip().rstrip(),
                    "partname": str(line[101 : (101 + 25)]).lstrip().rstrip(),
                    "vendor": factory,
                    "cd": cd,
                    "unit": unit,
                    "whs": factory,
                    "tagrp": "C",
                    "recisstype": recisstype,
                    "plantype": plantype,
                    "recid": str(line[0:4]).lstrip().rstrip(),
                    "aetono": str(line[4 : (4 + 12)]).lstrip().rstrip(),
                    "aetodt": str(line[16 : (16 + 10)]).lstrip().rstrip(),
                    "aetctn": float(str(line[26 : (26 + 9)]).lstrip().rstrip()),
                    "aetfob": float(str(line[35 : (35 + 9)]).lstrip().rstrip()),
                    "aenewt": float(str(line[44 : (44 + 11)]).lstrip().rstrip()),
                    "aentun": str(line[55 : (55 + 5)]).lstrip().rstrip(),
                    "aegrwt": float(str(line[60 : (60 + 11)]).lstrip().rstrip()),
                    "aegwun": str(line[71 : (71 + 5)]).lstrip().rstrip(),
                    "aeypat": str(line[76 : (76 + 25)]).lstrip().rstrip(),
                    "aeedes": str(
                        self.__check_partname(
                            factory, self.__re_partname(line[101 : (101 + 25)])
                        )
                    ),
                    "aetdes": str(
                        self.__check_partname(
                            factory, self.__re_partname(line[101 : (101 + 25)])
                        )
                    ),
                    "aetarf": float(str(line[151 : (151 + 10)]).lstrip().rstrip()),
                    "aestat": float(str(line[161 : (161 + 10)]).lstrip().rstrip()),
                    "aebrnd": float(str(line[171 : (171 + 10)]).lstrip().rstrip()),
                    "aertnt": float(str(line[181 : (181 + 5)]).lstrip().rstrip()),
                    "aetrty": float(str(line[186 : (186 + 5)]).lstrip().rstrip()),
                    "aesppm": float(str(line[191 : (191 + 5)]).lstrip().rstrip()),
                    "aeqty1": float(str(line[196 : (196 + 9)]).lstrip().rstrip()),
                    "aeqty2": float(str(line[205 : (205 + 9)]).lstrip().rstrip()),
                    "aeuntp": float(str(line[214 : (214 + 9)]).lstrip().rstrip()),
                    "aeamot": float(str(line[223 : (223 + 11)]).lstrip().rstrip()),
                    "plnctn": float(str(line[26 : (26 + 9)]).lstrip().rstrip()),
                    "plnqty": float(str(line[196 : (196 + 9)]).lstrip().rstrip()),
                    "minimum": 0,
                    "maximum": 0,
                    "picshelfbin": "PNON",
                    "stkshelfbin": "SNON",
                    "ovsshelfbin": "ONON",
                    "picshelfbasicqty": 0,
                    "outerpcs": 0,
                    "allocateqty": 0,
                    "sync": False,
                    "updatedon": datetime.now(),
                }

    def header_orderplan(self, fileName):
        fac = fileName[fileName.find("SPL") - 2 : fileName.find("SPL") - 1]
        plantype = "ORDERPLAN"
        cd = 20
        unit = "BOX"
        sortg1 = "PARTTYPE"
        sortg2 = "PARTNO"
        sortg3 = ""
        factory = "INJ"
        
        if fac != "5":
            factory = "AW"
            plantype = "ORDERPLAN"
            cd = 10
            unit = "COIL"
            sortg1 = "PONO"
            sortg2 = "PARTTYPE"
            sortg3 = "PARTNO"
            
        return {
            'factory': factory,
            'plantype': plantype,
            'cd': cd,
            'unit': unit,
            'sortg1': sortg1,
            'sortg2': sortg2,
            'sortg3': sortg3,
        }  
            
    def read_orderplan(self, obj, line):
        plantype = obj['plantype']
        cd = obj['cd']
        unit = obj['unit']
        sortg1 = obj['sortg1']
        sortg2 = obj['sortg2']
        sortg3 = obj['sortg3']
        factory = obj['factory']
        oqty = str(self.__trim_txt(line[89 : (89 + 9)]))
        if oqty == "":
            oqty = 0

        return {
                "vendor": factory,
                "cd": cd,
                "unit": unit,
                "whs": factory,
                "tagrp": "C",
                "factory": factory,
                "sortg1": sortg1,
                "sortg2": sortg2,
                "sortg3": sortg3,
                "plantype": plantype,
                "orderid": str(self.__trim_txt(line[13 : (13 + 15)])),
                # remove space
                "pono": str(self.__pono(line[13 : (13 + 15)])),
                "recid": str(self.__trim_txt(line[0:4])),
                "biac": str(self.__trim_txt(line[5 : (5 + 8)])),
                "shiptype": str(self.__trim_txt(line[4 : (4 + 1)])),
                "etdtap": datetime.strptime(
                    str(self.__trim_txt(line[28 : (28 + 8)])), "%Y%m%d"
                ),
                "partno": str(self.__trim_txt(line[36 : (36 + 25)])),
                "partname": str(
                    self.__check_partname(
                        factory,
                        self.__pono(line[61 : (61 + 25)]),
                    )
                ),
                "pc": str(self.__trim_txt(line[86 : (86 + 1)])),
                "commercial": str(self.__trim_txt(line[87 : (87 + 1)])),
                "sampleflg": str(self.__trim_txt(line[88 : (88 + 1)])),
                "orderorgi": int(oqty),
                "orderround": int(str(self.__trim_txt(line[98 : (98 + 9)]))),
                "firmflg": str(self.__trim_txt(line[107 : (107 + 1)])),
                "shippedflg": str(self.__trim_txt(line[108 : (108 + 1)])),
                "shippedqty": float(str(self.__trim_txt(line[109 : (109 + 9)]))),
                "ordermonth": datetime.strptime(
                    str(self.__trim_txt(line[118 : (118 + 8)])), "%Y%m%d"
                ),
                "balqty": float(str(self.__trim_txt(line[126 : (126 + 9)]))),
                "bidrfl": str(self.__trim_txt(line[135 : (135 + 1)])),
                "deleteflg": str(self.__trim_txt(line[136 : (136 + 1)])),
                "ordertype": str(self.__trim_txt(line[137 : (137 + 1)])),
                "reasoncd": str(self.__trim_txt(line[138 : (138 + 3)])),
                "upddte": datetime.strptime(
                    str(self.__trim_txt(line[141 : (141 + 14)])), "%Y%m%d%H%M%S"
                ),
                "updtime": datetime.strptime(
                    str(self.__trim_txt(line[141 : (141 + 14)])), "%Y%m%d%H%M%S"
                ),
                "carriercode": str(self.__trim_txt(line[155 : (155 + 4)])),
                "bioabt": int(str(self.__trim_txt(line[159 : (159 + 1)]))),
                "bicomd": str(self.__trim_txt(line[160 : (160 + 1)])),
                "bistdp": float(str(self.__trim_txt(line[165 : (165 + 9)]))),
                "binewt": float(str(self.__trim_txt(line[174 : (174 + 9)]))),
                "bigrwt": float(str(self.__trim_txt(line[183 : (183 + 9)]))),
                "bishpc": str(self.__trim_txt(line[192 : (192 + 8)])),
                "biivpx": str(self.__trim_txt(line[200 : (200 + 2)])),
                "bisafn": str(self.__trim_txt(line[202 : (202 + 6)])),
                "biwidt": float(str(self.__trim_txt(line[212 : (212 + 4)]))),
                "bihigh": float(str(self.__trim_txt(line[216 : (216 + 4)]))),
                "bileng": float(str(self.__trim_txt(line[208 : (208 + 4)]))),
                "lotno": str(self.__trim_txt(line[220 : (220 + 8)])),
                "minimum": 0,
                "maximum": 0,
                "picshelfbin": "PNON",
                "stkshelfbin": "SNON",
                "ovsshelfbin": "ONON",
                "picshelfbasicqty": 0,
                "outerpcs": 0,
                "allocateqty": 0,
                "sync": False,
                "updatedon": datetime.strptime(
                    str(self.__trim_txt(line[141 : (141 + 14)])), "%Y%m%d%H%M%S"
                ),
        }
    
    def get_file(self, name, fileName):
        is_success = True
        try:
            url = f"{str(self.host).replace('/api/v1', '')}{fileName}"
            payload={}
            headers = {}
            response = requests.request("GET", url, headers=headers, data=payload)
     
            ### create temp file
            f = open(name, mode='w+', encoding='utf-8')
            f.write(str(response.text).replace('\n', ''))
            f.close()
            
            return name
        
        except Exception as ex:
            pass
        
        
        return is_success
    
    def update_status(self, token, batchId, status=0):
        url = f"{self.host}/gedi/update/{batchId}"
        payload=f'is_downloaded={status}&is_active=1'
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        response = requests.request("PUT", url, headers=headers, data=payload)

        if response.status_code == 200:
            return True
        
        return False
    
    def get_receive(self, token, status=0):
        url = f"{self.host}/receive/header/index/{status}"
        payload={}
        headers = {
            'Authorization': f'Bearer {token}'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code != 200:return False
        data = response.json()
        return data['data']
    
    def update_receive_ent(self, token, receive_id, is_sync=0, status=1):
        url = f"{self.host}/receive/header/update/{receive_id}"
        payload=f'receive_sync={is_sync}&active={status}'
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        response = requests.request("PUT", url, headers=headers, data=payload)
        if response.status_code != 200:return False
        data = response.json()
        return data['data']
    
    def get_receive_body(self, token, receive_id, status=1):
        url = f"{self.host}/receive/body/index/{status}/{receive_id}"
        payload={}
        headers = {
            'Authorization': f'Bearer {token}'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code != 200:
            return False
        
        data = response.json()
        return data['data']
    

    def get_order_plan(self, token, limit=100, is_sync=0, status=1):
        url = f"{self.host}/order/plan/index/{status}/{is_sync}/{limit}"
        payload={}
        headers = {
            'Authorization': f'Bearer {token}'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code != 200:
            return False
        
        data = response.json()
        return data['data']
    
    def serial_no_tracking(self, token=None, obj=[]):
        try:
            url = f"{self.host}/trigger/store"
            payload = json.dumps({
                "whs": obj["whs"],
                "factory": obj["factory"],
                "rec_date": obj["rec_date"],
                "invoice_no": obj["invoice_no"],
                "part_no": obj["part_no"],
                "rvn_no": obj["rvmanagingno"],
                "serial_no": obj["serial_no"],
                "lot_no": obj["lot_no"],
                "case_id": obj["case_id"],
                "case_no": obj["case_no"],
                "std_pack_qty": obj["std_pack_qty"],
                "qty": obj["qty"],
                "shelve": obj["shelve"],
                "pallet_no": obj["pallet_no"],
                "on_stock_ctn": obj["on_stock_ctn"],
                "event_trigger": obj["event_trigger"]
            })
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            requests.request("POST", url, headers=headers, data=payload)
            
        except Exception as ex:
            LogActivity(name='SPL', subject="SERIAL TRACKING", status='Error',message=str(ex))
            pass
        
        return True