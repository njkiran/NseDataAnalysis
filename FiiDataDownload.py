import pandas as pd
import requests
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

base = 'C:\\JAY\\Data\\FII\\'
t = datetime.today().date()
dmonth={'01':'Jan','02':'Feb','03':'Mar','04':'Apr','05':'May','06':'Jun','07':'Jul','08':'Aug','09':'Sep','10':'Oct','11':'Nov','12':'Dec'}

ltdl =open(base+'log.txt','r')
lastdt = ltdl.read(10)
ltdl.close()
lastdt =datetime.strptime(lastdt,'%Y-%m-%d')
diff,wr = t-lastdt.date(),''

for i in range(1,diff.days+1):
    nextdt = lastdt+ relativedelta(days=i)
    d, m, y = '%02d' % nextdt.day, '%02d' % nextdt.month, '%02d' % nextdt.year
    if not os.path.isdir(base+y):
        os.mkdir(base+y)
    zpath = base+y+'\\fii_stats_'+d+'-'+dmonth[m]+'-'+y+'.xls'

    for i in range(7):
        while True:
            try:
                a=requests.get('https://www.nseindia.com/content/fo/fii_stats_'+d+'-'+dmonth[m]+'-'+y+'.xls')
            except requests.ConnectionError:
                print('No connection, retrying')
            break

    if a.status_code == 200:
        fdata = requests.get('https://www.nseindia.com/content/fo/fii_stats_'+d+'-'+dmonth[m]+'-'+y+'.xls')
        fo = open(zpath, 'wb')
        fo.write(fdata.content)
        fo.close()
        z, wr = (zpath, 'r'), nextdt.date()
        print(zpath)

# writing the last downloaded date to log.txt
if not isinstance(wr,str):
    ltdl=open(base+'log.txt','w')
    ltdl.write(str(wr))
    ltdl.close()