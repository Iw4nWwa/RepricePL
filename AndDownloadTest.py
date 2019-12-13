import csv
import json
import requests
import re
import mysql.connector
from datetime import date, timedelta

today = date.today()
yesterday = today - timedelta(days=1)

with open('credentials.json') as data_file:
    data = json.load(data_file)


app_id = 'com.orange.rn.dop'
report_type = 'daily_report'

params = {
  'api_token': data['api_token'],
  'from': yesterday,
  'to': yesterday
}

request_url = 'https://hq.appsflyer.com/export/{}/{}/v5'.format(app_id, report_type)

res = requests.request('GET', request_url, params=params)

if res.status_code != 200:
  if res.status_code == 404:
    print('There is a problem with the request URL. Make sure that it is correct')
  else:
    print('There was a problem retrieving data: ', res.text)
else:
  f = open('{}-{}-{}-to-{}.csv'.format(app_id, report_type, params['from'], params['to']), 'w', newline='', encoding="utf-8")
  f.write(res.text)
  f.close()



csv_file = ('{}-{}-{}-to-{}.csv'.format(app_id, report_type, params['from'], params['to']))
print('File named "' + csv_file + '" successfully captured')


def replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text


s_dic = {',':';','.':',', 'N/A': '0'}


with open(csv_file,'r') as f:
    text=next(f)
    text=f.read()
    text=replace_all(text,s_dic)
    text=re.sub(r'PROD\s(?=[1-9])',r'PROD',text)

with open('FlexAndroidYesterday.csv','w') as w:
    w.write(text)

print('Initial report processed and saved as FlexAndroidYesterday.')


andreport = 'FlexAndroidYesterday.csv'
cnx = mysql.connector.connect(
    user = data['user'],
    password = data['password'],
    host =  data['host'],
    database = data['database'])

cursor = cnx.cursor()

with open(andreport, mode='r') as csv_data:
    reader = csv.reader(csv_data, delimiter=';')
    csv_data_list = list(reader)
    for row in csv_data_list:
        cursor.execute("""
                   INSERT INTO flex(
                   date, Agency, MediaSource, Campaign, Impressions, \
                   Clicks, CTR, Installs, ConversionRate, Sessions, LoyalUser, \
                   LoyalUserByInstalls, TotalCost, AvarageeCPI, OSType)
                   VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Android')""",
                    (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13]))
cnx.commit()
cursor.close()
cnx.close()
print("Insertion completed successfully.")