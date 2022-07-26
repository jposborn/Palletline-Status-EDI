import requests
import xml.etree.cElementTree as et
import csv
import pandas as pd
import datetime
import os
import configparser
from ftplib import FTP
import glob
import shutil
import smtplib
from email.mime.text import MIMEText
import sys
import logging
import re
import json
import pyodbc
import time
import traceback
from tabulate import tabulate

# Todo ----- Version 2 Plans -----
#   Add Multi Depot Handling
#   Add POD handing for Laser Jobs where original reference is not a Laser ref (Save original csv for ref lookup )


def getpalletlinedata(pldepot):

    print("Current Time: " + currentdt)
    print("Last Run: " + lastrun)

    webuser = 'p' + pldepot + 'status'
    webpass = webuser

    print(webuser)
    print(webpass)

    params = {'vUserID': webuser, 'vPassword': webpass, 'TimeStampStart': lastrun, 'TimeStampEnd': currentdt}

    try:
        response_d = requests.get(statusdetailedurl, params=params)
        root_d = et.fromstring(response_d.content)
    except:
        print(e)
        errortype = "Unable to connect to Palletline Web Service"
        logging.error(errortype)
        sendemail(errortype)
        sys.exit(1)

    status_detailed_df = pd.DataFrame(
        columns=['ConNo', 'Ref2', 'StatusCode', 'StatusDate', 'StatusTime', 'PalletID', 'Key'])

    for element in root_d.findall('.//StatusQueryDetailed'):

        ReqDepot_d = element.find('ReqDepot').text
        if ReqDepot_d == pldepot:
            connumber_d = element.find('ConNo').text
            try:
                custref_d = element.find('Ref2').text
            except AttributeError:
                custref_d = ''
            statuscode_d = element.find('StatusCode').text
            statusdate_d = element.find('StatusDate').text
            statustime_d = element.find('StatusTime').text
            palletid_d = element.find('PalletID').text
            key_d = str(palletid_d) + str(statuscode_d)

            statrow_d = {'ConNo': connumber_d, 'Ref2': custref_d, 'StatusCode': statuscode_d,
                         'StatusDate': statusdate_d,
                         'StatusTime': statustime_d, 'PalletID': palletid_d, 'Key': key_d}
            status_detailed_df = status_detailed_df.append(statrow_d, ignore_index=True)

    status_detailed_df = status_detailed_df.drop_duplicates(['PalletID', 'StatusCode'])
    print('Status Detailed')
    print(tabulate(status_detailed_df, headers='keys', tablefmt='psql'))

    try:
        response_s = requests.get(statussummaryurl, params=params)
        root_s = et.fromstring(response_s.content)
    except:
        print(e)
        errortype = "Unable to connect to Palletline Web Service"
        logging.error(errortype)
        sendemail(errortype)
        sys.exit(1)

    status_summary_df = pd.DataFrame(columns=['PalletID', 'StatusCode', 'PODName', 'Key'])
    for element in root_s.findall('.//StatusQuerySummary'):
        statuscode_s = element.find('StatusCode').text
        ReqDepot_s = element.find('ReqDepot').text
        if ReqDepot_s == pldepot and statuscode_s == 'EPOD':
            palletid_s = element.find('PalletID').text
            podname_s = element.find('PODName').text
            key_s = str(palletid_s) + str(statuscode_s)
            statrow_s = {'PalletID': palletid_s, 'StatusCode': statuscode_s, 'PODName': podname_s, 'Key': key_s}

        elif ReqDepot_s == pldepot and statuscode_s == 'MPOD':
            palletid_s = element.find('PalletID').text
            podname_s = element.find('Notes').text
            podname_s = podname_s.replace('Signed By: ', '')
            key_s = str(palletid_s) + str(statuscode_s)
            statrow_s = {'PalletID': palletid_s, 'StatusCode': statuscode_s, 'PODName': podname_s, 'Key': key_s}

        else:
            palletid_s = element.find('PalletID').text
            podname_s = None
            key_s = str(palletid_s) + str(statuscode_s)
            statrow_s = {'PalletID': palletid_s, 'StatusCode': statuscode_s, 'PODName': podname_s, 'Key': key_s}

        status_summary_df = status_summary_df.append(statrow_s, ignore_index=True)

    print('Status Summary')
    status_summary_df = status_summary_df.drop_duplicates(['PalletID', 'StatusCode'])

    print(tabulate(status_summary_df, headers='keys', tablefmt='psql'))

    status_df = pd.merge(status_detailed_df, status_summary_df, how="left", on=['Key', 'Key'])

    print(tabulate(status_df, headers='keys', tablefmt='grid'))

    return status_df


def dataclean(df):

    print(df.dtypes)

    # Convert Laser Ref and CustRef Column to String
    convert_dict = {'ConNo': str, 'Ref2': str}
    df = df.astype(convert_dict)
    print(df.dtypes)

    # Check CustRef for Laser Jobs. If found swap values in LaserRef & CustRef
    r = re.compile(r'^[0-9][A-Za-z]{3}[0-9]{6}')
    islaser = df.Ref2.apply(lambda x: bool(r.match(x)))

    df['IsLaser'] = islaser.values
    idx = (df['IsLaser'] == True)
    df.loc[idx, ['ConNo', 'Ref2']] = df.loc[idx, ['Ref2', 'ConNo']].values

    # remove the trailing R if found in LaserRef column
    df['ConNo'] = df['ConNo'].str.extract(r'(^[0-9][A-Za-z]{3}[0-9]{6})', expand=False)

    # keep only rows that have a valid Laser Reference in LaserRef column
    df = df[df['ConNo'].astype(str).str.match("^[0-9][A-Za-z]{3}[0-9]{6}")]

    status_clean_df = df.drop_duplicates(['ConNo', 'StatusCode_x'])

    fcl_status_list = []
    fcl_status_desc_list = []
    for i in status_clean_df.index:
        fcl_status = findfclstatus(status_clean_df.loc[i, 'StatusCode_x'])
        fcl_status_list.append(fcl_status[0])
        fcl_status_desc_list.append(fcl_status[1])


    status_clean_df['FCLStatus'] = fcl_status_list
    status_clean_df['FCLStatusDesc'] = fcl_status_desc_list

    no_fcl_status = status_clean_df[status_clean_df['FCLStatus'] == 'xxx'].index
    status_clean_df.drop(no_fcl_status, inplace=True)

    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
    print(tabulate(status_clean_df, headers='keys', tablefmt='psql'))
    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')

    return status_clean_df


def findfclstatus(plcode):
    try:
        with open(statusidlookup, "r") as infile:
            reader = csv.reader(infile)
            next(reader)
            for line in reader:
                if [plcode] == line[:1]:
                    lascode = line[2]
                if [plcode] == line[:1]:
                    plcomment = line[1]
        infile.close()
        return [lascode, plcomment]
    except:
        errortype = "Unknown Palletline status " + plcode
        sendemail(errortype)
        logging.error(errortype)
        sys.exit(1)


def getpods(plinedepot):
    try:
        plftpuser = 'p' + plinedepot + 'podsdown'
        plftppassword = plftpuser
        ftp = FTP(plftpserver)
        ftp.login(plftpuser, plftppassword)
        filenames = ftp.nlst()
        for podfile in filenames:
            localfilename = os.path.join(podfolder, podfile)
            file = open(localfilename, 'wb')
            ftp.retrbinary('RETR ' + podfile, file.write)
            file.close()
        print(filenames)
        podnamecheck(filenames)
        podsendok = fclrenamesendpods()
        if podsendok == 0:
            for p in filenames:
                ftp.delete(p)
        ftp.quit()
    except:
        traceback.print_exc()
        errortype = "Failed to download POD's from Palletline"
        logging.error(errortype)
        sendemail(errortype)


def podnamecheck(filelist):
    pods = []
    pattern = re.compile("^[0-9][A-Za-z]{3}[0-9]{6}")
    for podfile in filelist:
        reference = podfile[4:][:10]
        if re.match(pattern, reference) is not None:
            print(podfile)
            print(reference)
            pods.append(podfile)
        else:
            plref = podfile[4:][:7]
            print(plref)
            # Get Laser Ref from Stirling
            stirlingquery = ("""SELECT szJobOrderNum " \
                                        "FROM JobItem " \
                                        "WHERE dwJobNumber = ?""")
            cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};" "Server=89.0.1.140;"
                                  "Database=Transport_Comp1;"
                                  "uid=svc_StirlingReporting;pwd=Report99")
            cursor = cnxn.cursor()
            cursor.execute(stirlingquery, plref)
            row = cursor.fetchone()

            print("Laser Ref from Stirling is " + (row[0]))
            laserpodfile = podfile[:4] + (row[0]) + podfile[11:]
            print(laserpodfile)
            reference2 = laserpodfile[4:][:10]
            if re.match(pattern, reference2) is not None:
                os.rename((podfolder + podfile), (podfolder + laserpodfile))
                pods.append(laserpodfile)
            else:
                os.remove(podfolder + podfile)
    return

            # todo  query Stirling for Laser Ref using plref
            #       build new pod file name with Laser Ref
            #       rename pod file


def fclrenamesendpods():
    os.chdir(podfolder)
    pattern = ".*\.tif$"
    for root, dirs, files in os.walk(podfolder):
        for file in filter(lambda x: re.match(pattern, x), files):
            os.remove(os.path.join(root, file))

    podlist = (glob.glob("*.*"))

    if podlist == []:
        logging.info("No Palletline POD's available")
        return 0

    for podfile in podlist:
        shutil.copy(podfile, archivefolder)

    fclpodlist = []
    suffix = []
    for t in range(len(podlist)):
        suffix.append((podlist[t])[-4:])
        files = '++01++' + ((podlist[t])[4:][:10]) + '++Vs1++PODREC' + suffix[t]
        fclpodlist.append(files)
        os.rename((podfolder + podlist[t]), (podfolder + fclpodlist[t]))
    print(suffix)
    print(podlist)
    print(fclpodlist)
    try:
        ftp = FTP(fclftpserver)
        ftp.login(fclftpuser, fclftppassword)
        ftp.cwd(fclpodftpfolder)
        for podfile in fclpodlist:
            file = open(podfile, 'rb')
            ftp.storbinary('STOR ' + podfile, file)
            file.close()
        ftp.quit()

        for a in fclpodlist:
            os.remove(podfolder + a)

        logging.info("Palletline POD's uploaded to FCL")
        return 0

    except Exception:
        traceback.print_exc()
        errortype = "Failed to send POD's to FCL"
        logging.error(errortype)
        sendemail(errortype)
        return 1


def fclstatusupdate(xmlfile, df):
    xf = open(programfolder + xmlfile, 'w')
    xf.write('<?xml version="1.0" encoding="UTF-8"?> \r')
    xf.write('<Message_Header> \r')

    for i in df.index:
        eventtime = (df['StatusTime'][i].replace(":", ""))[:-2]
        xf.write('    ' + '<Date_of_Message_Creation>' + str(datetime.date.today()) + '</Date_of_Message_Creation>')
        xf.write('    ' + '<Recipients_Identity>LASER</Recipients_Identity>')
        xf.write('    ' + '<Senders_Identity>PLI</Senders_Identity>')
        xf.write('    ' + '<Senders_Reference></Senders_Reference>')
        xf.write('    ' + '<Status>')
        xf.write('        ' + '<Event_Code>' + df['FCLStatus'][i] + '</Event_Code>')
        if df['FCLStatus'][i] == 'POD':
            event_comments = df['PODName'][i]
        else:
            event_comments = df['FCLStatusDesc'][i]
        xf.write('        ' + '<Event_Comments>' + str(event_comments) + '</Event_Comments>')
        xf.write('        ' + '<Event_Date>' + df['StatusDate'][i] + '</Event_Date>')
        xf.write('        ' + '<Event_Time>' + eventtime + '</Event_Time>')
        xf.write('        ' + '<Reference_Number_1>' + df['ConNo'][i] + '</Reference_Number_1>')
        xf.write('        ' + '<Reference_Type_1>OUR_BOOKING_REF</Reference_Type_1>')
        xf.write('        ' + '<Reference_Type_2>OVERSEAS_AGENT_REF</Reference_Type_2>')
        xf.write('    ' + '</Status>')
    xf.write('</Message_Header>')
    xf.close()
    return


def fclsendstatusxml(file, location):
    try:
        ftp = FTP(fclftpserver)
        ftp.login(fclftpuser, fclftppassword)
        ftp.cwd(fclediftpfolder)
        with open((location + file), 'rb') as p:
            ftp.storbinary('STOR ' + file, p)
        logging.info("Status file " + file + " uploaded to FCL")
        ftp.quit()
    except:
        errortype = "FTP Error: Status XML " + file + " failed FTP upload to FCL"
        logging.error(errortype)
        sendemail(errortype)
        ftp.quit()
        sys.exit(1)


def filearchive(proglocation, xmlfile):
    shutil.move((proglocation + xmlfile), (archivefolder + xmlfile))



def sendemail(error):

    msge = "The inbound Palletline EDI failed with the following error: \r\n" \
                                            "\r\n" \
                                            "######## " + error + " ########\r\n" \
                                            "\r\n" \
                                            "Please contact the IT department to investigate further" \
                                            " and re-transmit"

    msg = MIMEText(msge)

    msg['Subject'] = "Palletline EDI Error"
    msg['From'] = smtpsender
    msg['To'] = smtpreceiver

    s = smtplib.SMTP(smtpserver)
    s.send_message(msg)
    s.quit()
    return


# -------- MAIN PROGRAM ----------

config = configparser.ConfigParser()
config.read('PLConfig.ini')
depots = json.loads(config.get("DEPOT", "depots"))
archivefolder = config['FOLDERS']['archive']
programfolder = config['FOLDERS']['program']
podfolder = config['FOLDERS']['podfolder']
plftpserver = config['FTP']['plserver']
plftpfolder = config['FTP']['plfolder']
fclftpserver = config['FTP']['fclserver']
fclftpuser = config['FTP']['fcluser']
fclftppassword = config['FTP']['fclpassword']
fclpodftpfolder = config['FTP']['fclpodfolder']
fclediftpfolder = config['FTP']['fcledifolder']
emailserver = config['EMAIL']['server']
emailsender = config['EMAIL']['sender']
emailreceiver = config['EMAIL']['receiver']
statusdetailedurl = config['WEB SERVICE']['statusdetailedurl']
statussummaryurl = config['WEB SERVICE']['statussummaryurl']
statusidlookup = config['FILES']['statusidmap']
smtpserver = config['EMAIL']['server']
smtpsender = config['EMAIL']['sender']
smtpreceiver = config['EMAIL']['receiver']
loglevel = config['LOGGING']['level']

e = datetime.datetime.now()
currentdt = (e.strftime("%Y-%m-%d %H:%M:%S"))

f = open(programfolder + 'lastrun.txt', 'r')
lastrun = f.read()
f.close()

logging.basicConfig(
    filename='PLInterface.log', format='%(asctime)s:%(levelname)s:%(message)s', level=logging.getLevelName(loglevel))

print(depots)

for depot in depots:
    depot = str(depot)
    print(depot)
    e = datetime.datetime.now()
    currentdt = (e.strftime("%Y-%m-%d %H:%M:%S"))

    runtime = currentdt.replace(":", "")
    runtime1 = runtime.replace(" ", "")
    fileoutname = 'STAT_PL_' + depot + '_' + (runtime1.replace('-', '')) + '.csv'
    xmlfilename = fileoutname[:-4] + '.xml'

    # Check for no data
    status_data = getpalletlinedata(depot)

    if not status_data.empty:
        cleaned_status_data = dataclean(status_data)
        fclstatusupdate(xmlfilename, cleaned_status_data)
        fclsendstatusxml(xmlfilename, programfolder)
        filearchive(programfolder, xmlfilename)
    else:
        logging.info("No Palletline status updates for Depot: " + depot + " available")

    getpods(depot)

f = open(programfolder + 'lastrun.txt', 'w')
f.write(currentdt)
f.close()

time.sleep(10)
