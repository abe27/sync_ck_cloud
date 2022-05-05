from shareplum import Office365
from shareplum import Site
from shareplum.site import Version
    
authcookie = Office365('https://seiwapioneerlogistics.sharepoint.com', username='', password='').GetCookies()
site = Site('https://seiwapioneerlogistics.sharepoint.com/sites/CKGEDI', version=Version.v365, authcookie=authcookie);
folder = site.Folder('Shared Documents/GEDI')
with open("OES.WHAE.32T5.SPL20220505093708.TXT", mode='rb') as file:
        fileContent = file.read()
        
folder.upload_file(fileContent, "OES.WHAE.32T5.SPL20220505093708.TXT")