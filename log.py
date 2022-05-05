from datetime import datetime
class LogActivity:
    def __init__(self, name='CK',subject=None, status='Active',message=None):
        d = datetime.now().strftime('%Y%m%d')
        filename = f"{name}-{d}.log"
        f = open(filename, 'a+')
        txt = f"{str(datetime.now().strftime('%Y%m%d %H:%M:%S')).ljust(25)}SUBJECT: {str(subject).ljust(20)}STATUS: {str(status).ljust(10)}MESSAGE: {str(message).ljust(50)}"
        f.write(f"{txt}\n")
        f.close()