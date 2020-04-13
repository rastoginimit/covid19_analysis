from urllib.request import urlopen
from urllib.request import urlretrieve
import ssl
import re 
from pathlib import Path

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url = 'https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports/'
html = urlopen(url, context=ctx).read()
all_reports = re.findall(b'href=\"(.*?\.pdf)\?', html)
base_path = Path(__file__).parent

for report in all_reports:
    pdf_url = "https://www.who.int" + report.decode('utf8')
    report_name = pdf_url.split("/")[-1]
    report_rel_path = "./data/"+report_name
    report_file  = (base_path / report_rel_path).resolve()

    if not report_file.exists():
        #print(report_name)
        urlretrieve(pdf_url, report_file)