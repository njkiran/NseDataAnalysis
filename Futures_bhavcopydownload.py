import requests, zipfile, os, pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

base = 'C:\\JAY\\Data\\Bhavcopy\\'
t = datetime.today().date()
dmonth = {'01': 'JAN', '02': 'FEB', '03': 'MAR', '04': 'APR', '05': 'MAY', '06': 'JUN', '07': 'JUL', '08': 'AUG',
          '09': 'SEP', '10': 'OCT', '11': 'NOV', '12': 'DEC'}

# Before running this script , create a file called log.txt and write the date from which you want to download EOD data
# Opening file named log.txt , which keeps track of the last downloaded date.
ltdl = open(base + 'log.txt', 'r')
lastdt = ltdl.read(10)
ltdl.close()
lastdt = datetime.strptime(lastdt, '%Y-%m-%d')
diff, wr = t - lastdt.date(), ''

for i in range(1, diff.days + 1):
    nextdt = lastdt + relativedelta(days=i)
    d, m, y = '%02d' % nextdt.day, '%02d' % nextdt.month, '%02d' % nextdt.year
    if not os.path.isdir(base + y):
        os.mkdir(base + y)
        os.mkdir(base + y + '/Futures')
        os.mkdir(base + y + '/Futures/Text')
    zpath = base + y + '/' + d + '.zip'

    for i in range(7):
        while True:
            try:
                a = requests.get(
                    'https://nseindia.com/content/historical/EQUITIES/' + y + '/' + dmonth[m] + '/cm' + d + dmonth[
                        m] + y + 'bhav.csv.zip')
            except requests.ConnectionError:
                print('No connection, retrying')
            break

    if a.status_code == 200:
        futures = requests.get(
            'https://nseindia.com/content/historical/DERIVATIVES/' + y + '/' + dmonth[m] + '/fo' + d + dmonth[
                m] + y + 'bhav.csv.zip')
        fo = open(zpath, 'wb')
        fo.write(futures.content)
        fo.close()
        z, wr = zipfile.ZipFile(zpath, 'r'), nextdt.date()
        z.extractall(base + y + '/Futures')
        z.close()
        os.remove(zpath)

        # Read data from csv
        fpath = base + y + '/Futures' + '/'
        df = pd.read_csv(fpath + '/fo' + d + dmonth[m] + y + 'bhav.csv',
                         delimiter=',')  # Remove index column, index_col =0
        df.drop(df.columns[df.columns.str.contains('unnamed', case=False)], axis=1,
                inplace=True)  # Remove unnamed column
        df = df[(df['INSTRUMENT'] == "FUTIDX") | (df['INSTRUMENT'] == "FUTSTK")]

        # To convert timestamp into date time and modify in Amibroker required format
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        for ind in range(0, len(df)):
            de = '%02d' % df.iloc[ind, 14].day
            mh = '%02d' % df.iloc[ind, 14].month
            yr = df.iloc[ind, 14].year
            df.iloc[ind, 14] = str(yr) + str(mh) + str(de)

        # Update symbol with continuous futures contract
        arr = df['SYMBOL'].unique()
        for itr in range(0, len(arr)):
            sym = arr[itr]
            cntr = 0
            for lp in range(0, len(df)):
                if ((sym == df.iloc[lp, 1]) & (cntr < 3)):
                    if cntr == 0:
                        df.iloc[lp, 1] = df.iloc[lp, 1] + "-I"
                        cntr = (cntr * 1) + 1
                    elif cntr == 1:
                        df.iloc[lp, 1] = df.iloc[lp, 1] + "-II"
                        cntr = (cntr * 1) + 1
                    elif cntr == 2:
                        df.iloc[lp, 1] = df.iloc[lp, 1] + "-III"

        filename = 'C:\\JAY\\Data\\Bhavcopy\\2019\\Futures\\Text\\' + d + dmonth[m] + y + 'bhav.txt'
        df = (df[['SYMBOL', 'TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'CONTRACTS', 'OPEN_INT']])
        df.to_csv(filename, header=None, index=None, sep=',', mode='a')
        print(filename)

# writing the last downloaded date to log.txt
if not isinstance(wr, str):
    ltdl = open(base + 'log.txt', 'w')
    ltdl.write(str(wr))
    ltdl.close()