# -*- coding: utf-8 -*-
"""
Created on Fri Feb 03 15:13:59 2017

@author: LUL
"""
import pandas as pd
import numpy as np
import platform
import pandas.io.sql as psql

curOS = platform.system()

if curOS == "Windows":
    import MySQLdb as mysqlDriver
elif curOS == "Linux":
    import pymysql as mysqlDriver


max_tard = 10.

def get_num_monitored(ts_release):
    ##c/o Meryll
    if ts_release.hour == 20:
        return 4
    if ts_release.hour == 16:
        return 4
    if ts_release.hour == 12:
        return 6

def ts_release_window(ts_release,num_monitored):
    ###Computes for the timestamp of the prescribed earliest and latest EWI release
    ###INPUT: ts_release - expected time of release of EWI, num_monitored - number of monitored sites
    ###OUTPUT: earliest time, latest time
    ts_release = pd.to_datetime(ts_release)
    if ts_release.hour == 12:
        ts_offset_start = pd.Timedelta(10,unit = 'm')
        ts_offset_end = pd.Timedelta(5 + num_monitored + max_tard, unit = 'm')
    else:
        ts_offset_start = pd.Timedelta(5,unit = 'm')
        ts_offset_end = pd.Timedelta(num_monitored + max_tard, unit = 'm')
    return ts_release - ts_offset_start, ts_release + ts_offset_end

def get_last_ts_ewi_sent(ts_release,num_monitored):
    ts_start, ts_end = ts_release_window(ts_release,num_monitored)
    Hostdb = '192.168.1.100'
    Userdb = "root"
    Passdb = "senslope"
    Namedb = "senslopedb"
    ts_start = ts_start.strftime('%Y-%m-%d %H:%M:%S')
    ts_end = ts_end.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=Namedb)

    except mysqlDriver.OperationalError:
        print '.',
    
    query = "SELECT DISTINCT timestamp_written FROM smsoutbox WHERE sms_id >= (SELECT max(sms_id) - 10000 FROM smsoutbox) AND timestamp_written >= '{}' AND timestamp_written <= '{}' AND sms_msg LIKE '%ang alert level%' AND NOT(sms_msg LIKE '%ang alert level sa inyong lugar%') ORDER by sms_id asc".format(ts_start,ts_end)
    all_ts = psql.read_sql(query, db)
    db.close()
    
    if len(all_ts) < num_monitored:
        return 'Incomplete EWI Sent'
    else:
        return all_ts.timestamp_written.values[-1]

def scoring_table(ts_baseline,last_ts):
    time_diff = (ts_baseline - last_ts)/np.timedelta64(1,'m')
    if time_diff >= 0:
        return round(10.00,2)
    elif time_diff <= -1*max_tard:
        return round(0.00,2)
    else:
        return round(10.00 + time_diff*(10/max_tard),2)

def get_ewi_release_ct_score(ts_release,num_monitored):
    ts_release = pd.to_datetime(ts_release)
    
    if ts_release.hour == 12:
        ts_baseline = ts_release + pd.Timedelta(5 + num_monitored, unit = 'm')
    else:
        ts_baseline = ts_release + pd.Timedelta(num_monitored, unit = 'm')
    
    last_ts = get_last_ts_ewi_sent(ts_release,num_monitored)
    
    if last_ts == 'Incomplete EWI Sent':
        return 0
    else:
        return scoring_table(ts_baseline,last_ts)

def get_total_ewi_release_ct_score(ts_shift):
    ts_shift = pd.to_datetime(ts_shift)
    scores = []
    for release_num in np.arange(1,4):
        ts_release = ts_shift + pd.Timedelta(release_num*4 + 0.5,'h')
        num_monitored = get_num_monitored(ts_release)
        scores.append(get_ewi_release_ct_score(ts_release,num_monitored))
        print "Score for {} release: {}".format(ts_release.strftime('%H:%M'),scores[release_num - 1])
    print "-------------------------\n"
    average_score = np.average(np.array(scores))
    print "Average score: {}".format(average_score)
    return average_score