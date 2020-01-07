import csv
import json
import requests
import re
import mysql.connector
from datetime import date, timedelta, datetime


today = date.today()
yesterday = today - timedelta(days=1)
now = datetime.now()
current_time = now.strftime("%H:%M:%S")

with open('credentials.json') as data_file:
    data = json.load(data_file)

app_id = 'id1441116618'
report_type = 'installs_report'

params = {
    'api_token': data['api_token'],
    'from': yesterday,
    'to': yesterday,
    'sfx': '&timezone=Europe%2fWarsaw&additional_fields=install_app_store,contributor1_match_type,contributor2_match_type,contributor3_match_type,match_type,device_category,gp_referrer,gp_click_time,gp_install_begin,amazon_aid,keyword_match_type&reattr=true'
}

request_url = 'https://hq.appsflyer.com/export/{}/{}/v5?api_token={}{}'.format(app_id, report_type, data['api_token'],params['sfx'])

res = requests.request('GET', request_url, params=params)

if res.status_code != 200:
    if res.status_code == 404:
        print('There is a problem with the request URL. Make sure that it is correct')
    else:
        print('There was a problem retrieving data: ', res.text)
else:
    f = open('Installs_report-{}-{}-{}-to-{}-iOSRet.csv'.format(app_id, report_type, params['from'], params['to']), 'w', newline='',
             encoding="utf-8")
    f.write(res.text)
    f.close()

print(request_url)

csv_file = ('Installs_report-{}-{}-{}-to-{}-iOSRet.csv'.format(app_id, report_type, params['from'], params['to']))
print(current_time, ': File named :"' + csv_file + '" successfully captured.')

with open(csv_file, 'r', encoding='utf-8') as f:
    with open('Installs_report_converted-iOSRet.csv', 'w', encoding='utf-8') as f1:
        next(f)  # skip header line
        for line in f:
            f1.write(line)

print(current_time, ': Initial report processed and saved as : Installs_report_converted-iOSRet.csv')

andreport = 'Installs_report_converted-iOSRet.csv'
print(current_time, ': printing andreport file name:' + andreport)



cnx = mysql.connector.connect(
    user=data['user'],
    password=data['password'],
    host=data['host'],
    database=data['database'])

cursor = cnx.cursor()

with open(andreport, mode='r', encoding='utf-8') as csv_data:
    reader = csv.reader(csv_data, delimiter=',')
    csv_data_list = list(reader)
    for row in csv_data_list:
        cursor.execute("""
                   INSERT INTO flex_retargeting_conversions(
                        AttributedTouchType,
                        AttributedTouchTime,
                        InstallTime,
                        EventTime,
                        EventName,
                        EventValue,
                        EventRevenue,
                        EventRevenueCurrency,
                        EventRevenueUSD,
                        EventSource,
                        IsReceiptValidated,
                        Partner,
                        MediaSource,
                        Channel,
                        Keywords,
                        Campaign,
                        CampaignID,
                        Adset,
                        AdsetID,
                        Ad,
                        AdID,
                        AdType,
                        SiteID,
                        SubSiteID,
                        SubParam1,
                        SubParam2,
                        SubParam3,
                        SubParam4,
                        SubParam5,
                        CostModel,
                        CostValue,
                        CostCurrency,
                        Contributor1Partner,
                        Contributor1MediaSource,
                        Contributor1Campaign,
                        Contributor1TouchType,
                        Contributor1TouchTime,
                        Contributor2Partner,
                        Contributor2MediaSource,
                        Contributor2Campaign,
                        Contributor2TouchType,
                        Contributor2TouchTime,
                        Contributor3Partner,
                        Contributor3MediaSource,
                        Contributor3Campaign,
                        Contributor3TouchType,
                        Contributor3TouchTime,
                        Region,
                        CountryCode,
                        State,
                        City,
                        PostalCode,
                        DMA,
                        IP,
                        WIFI,
                        Operator,
                        Carrier,
                        Language,
                        AppsFlyerID,
                        AdvertisingID,
                        IDFA,
                        AndroidID,
                        CustomerUserID,
                        IMEI,
                        IDFV,
                        Platform,
                        DeviceType,
                        OSVersion,
                        AppVersion,
                        SDKVersion,
                        AppID,
                        AppName,
                        BundleID,
                        IsRetargeting,
                        RetargetingConversionType,
                        AttributionLookback,
                        ReengagementWindow,
                        IsPrimaryAttribution,
                        UserAgent,
                        HTTPReferrer,
                        OriginalURL,
                        InstallAppStore,
                        Contributor1MatchType,
                        Contributor2MatchType,
                        Contributor3MatchType,
                        MatchType,
                        DeviceCategory,
                        GooglePlayReferrer,
                        GooglePlayClickTime,
                        GooglePlayInstallBeginTime,
                        AmazonFireID,
                        KeywordMatchType
                   )
                   VALUES(
                   %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                       (
                           row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                           row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20],
                           row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29], row[30],
                           row[31], row[32], row[33], row[34], row[35], row[36], row[37], row[38], row[39], row[40],
                           row[41], row[42], row[43], row[44], row[45], row[46], row[47], row[48], row[49], row[50],
                           row[51], row[52], row[53], row[54], row[55], row[56], row[57], row[58], row[59], row[60],
                           row[61], row[62], row[63], row[64], row[65], row[66], row[67], row[68], row[69], row[70],
                           row[71], row[72], row[73], row[74], row[75], row[76], row[77], row[78], row[79], row[80],
                           row[81], row[82], row[83], row[84], row[85], row[86], row[87], row[88], row[89], row[90],
                           row[91]

                       )
                       )
cnx.commit(),
cursor.close()
cnx.close()
print(current_time, ': Insertion completed successfully.')
