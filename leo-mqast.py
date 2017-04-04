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

from operator import is_not
from functools import partial


max_tard = 10.

Hostdb = '192.168.150.127'
Userdb = "root"
Passdb = "senslope"
Namedb = "senslopedb"

def get_num_monitored(ts_release):
    ##c/o Meryll
    if ts_release.hour == 20:
        return 6
    if ts_release.hour == 16:
        return 6
    if ts_release.hour == 12:
        return 0
    if ts_release.hour == 10:
        return 0
    else:
        return 6
        
def get_monitored_sites(ts_release):
    ##c/o Meryll
    return []

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

def ground_data_receive_window(ts_release):
    ts_release = pd.to_datetime(ts_release)
    ts_start = ts_release - pd.Timedelta(2,unit = 'h')
    ts_end = ts_release - pd.Timedelta(2,unit = 'h')
    return ts_start,ts_end

def check_num_ground_data_sent(ts_release):
    ts_start, ts_end = ground_data_receive_window(ts_release)
    ts_start = ts_start.strftime('%Y-%m-%d %H:%M:%S')
    ts_end = ts_end.strftime('%Y-%m-%d %H:%M:%S')
    try:
        db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=Namedb)

    except mysqlDriver.OperationalError:
        print '.',
    
    query = "SELECT DISTINCT sms_msg FROM senslopedb.smsinbox WHERE sms_id >= (SELECT max(sms_id) - 40200 FROM senslopedb.smsinbox) AND timestamp >= '{}' AND timestamp <= '{}' AND (sms_msg LIKE 'EVENT%' OR sms_msg LIKE 'ROUTINE%') AND read_status = 'READ-SUCCESS'".format(ts_start,ts_end)
    all_msg = psql.read_sql(query, db)
    db.close()
    
    return len(all_msg)

def get_last_ts_ewi_sent(ts_release,num_monitored):
    ts_start, ts_end = ts_release_window(ts_release,num_monitored)
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

def get_last_ts_reminder_sent(ts_release,num_monitored):
    ts_start, ts_end = ts_release_window(ts_release,num_monitored)
    ts_start = ts_start.strftime('%Y-%m-%d %H:%M:%S')
    ts_end = ts_end.strftime('%Y-%m-%d %H:%M:%S')
    Hostdb = '192.168.150.127'
    Userdb = "root"
    Passdb = "senslope"
    Namedb = "senslopedb"
    try:
        db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=Namedb)

    except mysqlDriver.OperationalError:
        print '.',
    
    query = "SELECT DISTINCT timestamp_written FROM smsoutbox WHERE sms_id >= (SELECT max(sms_id) - 10000 FROM smsoutbox) AND timestamp_written >= '{}' AND timestamp_written <= '{}' AND (sms_msg LIKE '%paalala%' OR sms_msg LIKE '%remind%' OR sms_msg LIKE '%asahan%') AND NOT (sms_msg LIKE '%Monitoring shift reminder%' OR sms_msg LIKE 'Non reporting sites reminder%' OR sms_msg LIKE '%tracking%') ORDER by sms_id asc".format(ts_start,ts_end)
    all_ts = psql.read_sql(query, db)
    db.close()
    if len(all_ts) == 0:
        return 'No reminder sent'
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

def get_reminder_release_ct_score(ts_release,num_monitored):
    ts_release = pd.to_datetime(ts_release)
    num_ground_data = check_num_ground_data_sent(ts_release)
    if num_monitored == 0:
        return 10

    else:
        ts_baseline = ts_release + pd.Timedelta(num_monitored, unit = 'm')
        
        last_ts = get_last_ts_reminder_sent(ts_release,num_monitored)
        
        if last_ts == 'No reminder sent' and num_ground_data == 0:
            return 0
        elif last_ts == 'No reminder sent' and num_ground_data > 0:
            return 10
        else:
            return scoring_table(ts_baseline,last_ts)

def get_total_ewi_release_ct_score(ts_shift):
    ts_shift = pd.to_datetime(ts_shift)
    scores = []
    for release_num in np.arange(1,4):
        ts_release = ts_shift + pd.Timedelta(release_num*4 + 0.5,'h')
        num_monitored = get_num_monitored(ts_release)
        if num_monitored == 0:
            continue
        scores.append(get_ewi_release_ct_score(ts_release,num_monitored))
        print "Score for {} release: {}".format(ts_release.strftime('%H:%M'),scores[-1])
    print "-------------------------\n"
    average_score = np.average(np.array(scores))
    print "Average score: {}".format(average_score)
    return average_score

def get_total_reminder_release_ct_score(ts_shift):
    scores = []    
    ts_shift = pd.to_datetime(ts_shift)
    if ts_shift.hour == 7:
        for release_num in np.arange(1,3):
            ts_release = ts_shift + pd.Timedelta(release_num*4 - 1.5,'h')
            num_monitored = get_num_monitored(ts_release)
            if num_monitored == 0:
                continue
            scores.append(get_reminder_release_ct_score(ts_release,num_monitored))
            print "Score for {} release: {}".format(ts_release.strftime('%H:%M'),scores[-1])
    if ts_shift.hour == 19:
        ts_release = ts_shift + pd.Timedelta(10.5, 'h')
        num_monitored = get_num_monitored(ts_release)
        scores.append(get_reminder_release_ct_score(ts_release,num_monitored))
        print "Score for {} release: {}".format(ts_release.strftime('%H:%M'),scores[-1])
    print "-------------------------\n"
    average_score = np.average(np.array(scores))
    print "Average score: {}".format(average_score)
    return average_score

def get_alert_sms_sent(ts_shift,site):
    ts_shift = pd.to_datetime(ts_shift)
    ts_end = ts_shift + pd.Timedelta(13,'h')
    site = site.lower()
    try:
        db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=Namedb)

    except mysqlDriver.OperationalError:
        print '.',
    
    query = "SELECT DISTINCT timestamp_written FROM smsoutbox WHERE sms_id >= (SELECT max(sms_id) - 10000 FROM smsoutbox) AND timestamp_written >= '{}' AND timestamp_written <= '{}' AND (sms_msg LIKE '%paalala%' OR sms_msg LIKE '%remind%' OR sms_msg LIKE '%asahan%') AND NOT (sms_msg LIKE '%Monitoring shift reminder%' OR sms_msg LIKE 'Non reporting sites reminder%' OR sms_msg LIKE '%tracking%') ORDER by sms_id asc".format(ts_start,ts_end)
    all_ts = psql.read_sql(query, db)
    db.close()

def GetEWITimeWritten(ts_release,site):
    ts_release = pd.to_datetime(ts_release)    
    if ts_release.hour != 12:
        ts_end = pd.to_datetime(ts_release + pd.Timedelta(0.5,'h'))
        ts_release = pd.to_datetime(ts_release) - pd.Timedelta(0.25,'h')
    else:
        ts_end = pd.to_datetime(ts_release + pd.Timedelta(0.5,'h')+pd.Timedelta(minutes = 5))
        ts_release = pd.to_datetime(ts_release) - pd.Timedelta(0.25,'h')
    
    db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=Namedb)
    cur = db.cursor()
    cur.execute("USE {}".format(Namedb))

    query = "SELECT timestamp_written, timestamp_sent FROM smsoutbox WHERE sms_id >= (SELECT max(sms_id) - 50000 FROM smsoutbox) AND timestamp_written >= '{}' AND timestamp_written <= '{}' AND sms_msg LIKE '%{}%' AND sms_msg LIKE '%ang alert level%' AND NOT(sms_msg LIKE '%ang alert level sa inyong lugar%') ORDER by sms_id asc".format(ts_release.strftime("%Y-%m-%d %H:%M"),ts_end.strftime("%Y-%m-%d %H:%M"),site.title())
    cur.execute(query)
    
    try:
        result = cur.fetchall()
        db.close()
        result = zip(*result)
        return np.array([result[0],result[1]])
    except:
        return 'No EWI sent.'
        db.close()
    
def SiteToBgy(event_site):
    if event_site == 'lte':
        return 'lit'
    elif event_site == 'car':
        return 'carlos'
    elif event_site == 'mng':
        return 'man'
    elif event_site == 'msl':
        return 'mes'
    elif event_site == 'msu':
        return 'mes'
    elif event_site == 'png':
        return 'pan'
    else:
        return event_site
    
def GetReleaseTS(monitoring_event):
    #### Get datetime of event start and end
    event_start = monitoring_event.event_start
    event_end = monitoring_event.event_end
    event_site = monitoring_event.site
    
    #### Change site name
    event_site = SiteToBgy(event_site)
    
    #### Round to next hour if minutes > 0
    if event_start.minute > 0:
        event_start = event_start + pd.Timedelta(minutes = 60 - event_start.minute)
    
    #### Get offset time before next release
    next_release_timedelta = np.array((0,4.,8.,12.,16.,20.,24.)) - event_start.hour
    print "Checking event-based monitoring on \nSite {} from {} to {}...\n\n".format(event_site.upper(),event_start,event_end)
    try:
        next_release_timedelta = min(next_release_timedelta[next_release_timedelta > 0])
    except:
        next_release_timedelta = 0

    #### Round start time to nearest release time
    second_release = event_start + pd.Timedelta(next_release_timedelta,'h')    
    
    #### Release time is every 4 hours after second release
    release_times = np.insert(pd.date_range(second_release,event_end,freq = '4H').values,0,event_start)
    
    #### Get average delay
    missed_alerts = 0
    delay_written = pd.Timedelta(minutes = 0)
    delay_sent = 0
    
    #### Check the delay time
    for release_time in release_times:
        result = GetEWITimeWritten(release_time,event_site)
        print result
        if pd.to_datetime(release_time) <= pd.datetime(2017,01,15):
            continue

        if type(result) == type('No EWI sent.'):
            missed_alerts += 1
            print "Missed Alert for {} release\n".format(pd.to_datetime(release_time).strftime("%b %d %H:%M"))
            with open('missedalerts.csv','a') as macsv:
                z = pd.DataFrame(columns = ['site','release_time'])
                z.loc[0] = (event_site[:3],release_time)
                z.to_csv(macsv,header = False,index = False)
            continue

        ts_written = result[0][0]
        ts_sent = filter(partial(is_not,None),np.array(result[1]))
        
        if pd.to_datetime(release_time).hour == 12:
            cur_delay_written = pd.to_datetime(ts_written) - pd.to_datetime(release_time) - pd.Timedelta(minutes = 5)
            if cur_delay_written > pd.Timedelta(minutes = 0):
                delay_written = delay_written + cur_delay_written
        else:
            cur_delay_written = pd.to_datetime(ts_written) - pd.to_datetime(release_time)
            if cur_delay_written > pd.Timedelta(minutes = 0):
                delay_written = delay_written + cur_delay_written
        if np.average(map(lambda x:(pd.to_datetime(x) - pd.to_datetime(ts_written))/np.timedelta64(1,'m'),ts_sent)) > 0:
            delay_sent = delay_sent + np.average(map(lambda x:(pd.to_datetime(x) - pd.to_datetime(ts_written))/np.timedelta64(1,'m'),ts_sent))

    return pd.Series({'delay_written':delay_written/np.timedelta64(1,'m'),'delay_sent':delay_sent,'missed':missed_alerts,'total':len(release_times[release_times > np.datetime64('2017-01-15 20:00')])})
            

def GetStatsEventBasedMonitoring(event_csv_file):
    #### Get data of monitoring events from csv file
    monitoring_events = pd.read_csv(event_csv_file,parse_dates = ['event_start','event_end'])
    monitoring_events = monitoring_events[monitoring_events.site == 'sum']
    print monitoring_events
    results = monitoring_events.apply(GetReleaseTS,axis = 1)
    results = monitoring_events.merge(results,left_index = True, right_index = True)
    
    average_delay_written = results.delay_written.values*results.total.values/np.sum(results.total.values)
    average_delay_sent = results.delay_sent.values*results.total.values/np.sum(results.total.values)
    percent_missed_ewi = np.sum(results.missed.values)/np.sum(results.total.values)
    
    print results
    print "\n\n"
    print "-------------------------------------------\n"
    print "Average delay (written): {} minutes\n".format(round(average_delay_written),2)
    print "Average delay (sent): {} minutes\n".format(round(average_delay_sent),2)
    print "Total number of EWI releases: {}\n".format(results.total.values)
    print "Total number of missed: {}\n"
    #### Get timestamps of releases
    return results
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    