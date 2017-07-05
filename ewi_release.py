# -*- coding: utf-8 -*-
"""
Created on Fri Feb 03 15:13:59 2017

@author: LUL
"""
import pandas as pd
import numpy as np
import platform
import pandas.io.sql as psql
import querySenslopeDb as q
import datetime

curOS = platform.system()

if curOS == "Windows":
    import MySQLdb as mysqlDriver
elif curOS == "Linux":
    import pymysql as mysqlDriver

from operator import is_not
from functools import partial


max_tard = 10.

Hostdb = '192.168.150.127'
Hostdb2 = '192.168.150.129'
Userdb = "root"
Passdb = "senslope"
Namedb = "senslopedb"
event_csv_file = 'q1monitoringevents.csv'
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
    elif event_site == 'blc':
        return 'boloc'
    elif event_site == 'bto':
        return 'bato'
    else:
        return event_site

def SiteToBgy2(event_site):
    '''Returns the barangay of given site code'''
    
    #### Connect to db
    db = mysqlDriver.connect(host = Hostdb2, user = Userdb, passwd = Passdb, db=Namedb)
    cur = db.cursor()
    cur.execute("USE {}".format(Namedb))
    
    #### Query to sites table
    query = "SELECT brgy FROM sites WHERE code = '{}'".format(event_site)
    cur.execute(query)
    
    #### Get result
    brgy = cur.fetchone()[0]
    
    return brgy

def BgyToSite(brgy):
    
    #### Connect to db
    db = mysqlDriver.connect(host = Hostdb2, user = Userdb, passwd = Passdb, db=Namedb)
    cur = db.cursor()
    cur.execute("USE {}".format(Namedb))
    
    #### Query to sites table
    query = "SELECT code FROM sites WHERE brgy = '{}'".format(brgy)
    cur.execute(query)
    
    #### Get result
    code = cur.fetchone()[0]
    
    return code

def GetReleaseTS(monitoring_event):
    #### Get datetime of event start and end
    event_start = monitoring_event.event_start
    event_end = monitoring_event.event_end
    event_site = monitoring_event.site
    
    #### Change site name
    event_site = SiteToBgy2(event_site)
    
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

        if pd.to_datetime(release_time) <= pd.datetime(2017,01,15,20):
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
    results = monitoring_events.apply(GetReleaseTS,axis = 1)
    results = monitoring_events.merge(results,left_index = True, right_index = True)
    
    average_delay_written = np.sum(results.delay_written.values)/np.sum(results.total.values)
    average_delay_sent = np.sum(results.delay_sent.values)/np.sum(results.total.values)
    total_release = np.sum(results.total)
    total_missed = np.sum(results.missed)
    print results
    print "\n\n"
    print "Event-based Monitoring Statistics:\n"
    print "-------------------------------------------\n"
    print "Average delay (written): {} minutes\n".format(round(average_delay_written,2))
    print "Average delay (sent): {} minutes\n".format(round(average_delay_sent,2))
    print "Total number of EWI releases: {}\n".format(total_release)
    print "Total number of missed: {}\n".format(total_missed)
    print "Success rate: {}%".format(np.round((1 - total_missed / total_release)*100,2))
    #### Get timestamps of releases
    return results

def ComputeRoutineDelay(routine_monitoring_data):
    ts_written = pd.to_datetime(routine_monitoring_data.timestamp_written)
    ts_sent = pd.to_datetime(routine_monitoring_data.timestamp_sent)
    delay_written = ts_written - pd.datetime(ts_written.year,ts_written.month,ts_written.day,12,0)
    delay_sent = ts_sent - ts_written
    return pd.Series({'delay_written':delay_written/np.timedelta64(1,'m'),'delay_sent':delay_sent/np.timedelta64(1,'m')})
    
    

def GetStatsRoutineMonitoring(ts_start,ts_end):
    #### Get all routine monitoring messages
    routine_monitoring_data = pd.DataFrame(columns = ['timestamp_written'])
    db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=Namedb)
    cur = db.cursor()
    cur.execute("USE {}".format(Namedb))

    query = "SELECT timestamp_written, timestamp_sent FROM smsoutbox WHERE sms_id >= (SELECT max(sms_id) - 50000 FROM smsoutbox) AND timestamp_written >= '{}' AND timestamp_written <= '{}' AND sms_msg LIKE '%ang alert level sa inyong lugar%' AND NOT(sms_msg LIKE '%extended%') ORDER by sms_id asc ".format(pd.to_datetime(ts_start).strftime("%Y-%m-%d %H:%M"),pd.to_datetime(ts_end).strftime("%Y-%m-%d %H:%M"))
    cur.execute(query)
    
    results = cur.fetchall()
    db.close()
    results = zip(*results)
    
    routine_monitoring_data['timestamp_written'] = results[0]
    routine_monitoring_data['timestamp_sent'] = results[1]
    routine_monitoring_data.dropna(inplace = True)
    print routine_monitoring_data
    delay_times = routine_monitoring_data.apply(ComputeRoutineDelay,axis = 1)
    results = routine_monitoring_data.merge(delay_times,left_index = True,right_index = True)

    print results
    
    delay_written = results[results.delay_written >= 0].delay_written
    delay_sent = results[results.delay_sent >= 0].delay_sent    
    
    average_delay_written = np.sum(delay_written)/len(results)
    max_delay_written = max(delay_written)
    average_delay_sent = np.sum(delay_sent)/len(results)
    max_delay_sent = max(delay_sent)
    total_release = len(results)
    total_missed = len(delay_written[delay_written > 20])
    
    print "\n\n"
    print "Routine Monitoring Statistics:"
    print "-------------------------------------------\n"
    print "Date Start: {}".format(pd.to_datetime(ts_start).strftime("%b %d %Y"))
    print "Date End: {}".format(pd.to_datetime(ts_end).strftime("%b %d %Y"))
    print "Average delay (written): {} minutes\n".format(round(average_delay_written,2))
    print "Average delay (sent): {} minutes\n".format(round(average_delay_sent,2))
    
    print "Maximum delay (written): {} minutes\n".format(round(max_delay_written,2))
    print "Maximum delay (sent): {} minutes\n".format(round(max_delay_sent,2))
    print "Total number of EWI releases: {}\n".format(total_release)
    print "Total number of missed: {}\n".format(total_missed)
    print "Success rate: {}%".format(np.round((1 - total_missed / float(total_release))*100,2))
    #### Get timestamps of releases
    return results


def GetStatsRoutineMonitoring2(ts_start,ts_end):
    #### Get all routine monitoring messages
    routine_monitoring_data = pd.DataFrame(columns = ['timestamp_written'])
    db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=Namedb)
    cur = db.cursor()
    cur.execute("USE {}".format(Namedb))

    query = "SELECT timestamp_written, timestamp_sent FROM smsoutbox WHERE sms_id >= (SELECT max(sms_id) - 50000 FROM smsoutbox) AND timestamp_written >= '{}' AND timestamp_written <= '{}' AND sms_msg LIKE '%ang alert level sa inyong lugar%' AND NOT(sms_msg LIKE '%extended%') ORDER by sms_id asc ".format(pd.to_datetime(ts_start).strftime("%Y-%m-%d %H:%M"),pd.to_datetime(ts_end).strftime("%Y-%m-%d %H:%M"))
    cur.execute(query)
    
    results = cur.fetchall()
    db.close()
    results = zip(*results)
    
    routine_monitoring_data['timestamp_written'] = results[0]
    routine_monitoring_data['timestamp_sent'] = results[1]
    routine_monitoring_data.dropna(inplace = True)
    print routine_monitoring_data
    delay_times = routine_monitoring_data.apply(ComputeRoutineDelay,axis = 1)
    results = routine_monitoring_data.merge(delay_times,left_index = True,right_index = True)

    print results
    
    delay_written = results.delay_written
    delay_sent = results.delay_sent    
    
    average_delay_written = np.sum(delay_written)/len(results)
    max_delay_written = max(delay_written)
    min_delay_written = min(delay_written)
    average_delay_sent = np.sum(delay_sent)/len(results)
    sd_delay_written = np.sqrt(np.sum((delay_written - average_delay_written)**2)/ float(len(results)))
    min_delay_sent = min(delay_sent)
    max_delay_sent = max(delay_sent)
    sd_delay_sent = np.sqrt(np.sum((delay_sent - average_delay_sent)**2) / len(results))
    total_release = len(results)
    total_missed = len(delay_written[delay_written > 20])
    
    print "\n\n"
    print "Routine Monitoring Statistics:"
    print "-------------------------------------------\n"
    print "Date Start: {}".format(pd.to_datetime(ts_start).strftime("%b %d %Y"))
    print "Date End: {}\n".format(pd.to_datetime(ts_end).strftime("%b %d %Y"))
    print "SMS Written"
    print "Average delay: {} minutes".format(round(average_delay_written,2))
    print "Minimum delay: {} minutes".format(round(min_delay_written,2))
    print "Maximum delay: {} minutes".format(round(max_delay_written,2))
    print "Standard devation: {}\n".format(sd_delay_written)
    print "SMS Sent"
    print "Average delay: {} minutes".format(round(average_delay_sent,2))        
    print "Minimum delay: {} minutes".format(round(min_delay_sent,2))
    print "Maximum delay: {} minutes".format(round(max_delay_sent,2))
    print "Standard devation: {}\n".format(sd_delay_sent)

    print "Total number of EWI releases: {}".format(total_release)
    print "Total number of missed alerts: {}".format(total_missed)
    print "Success rate: {}%\n".format(np.round((1 - total_missed / float(total_release))*100,2))
    
    print "Frequency Distribution:\n"
    
    print "SMS Written:"
    print "less than zero     - {}".format(len(delay_written[delay_written <= 0]))
    print "zero to 10 minutes - {}".format(len(delay_written[np.logical_and(delay_written>0,delay_written<=10)]))
    print "10 to 20 minutes   - {}".format(len(delay_written[np.logical_and(delay_written>10,delay_written<=20)]))
    print "20 to 30 minutes   - {}".format(len(delay_written[np.logical_and(delay_written>20,delay_written<=30)]))
    print "greater than 30    - {}\n".format(len(delay_written[delay_written > 30]))
    
    print "SMS Sent:"
    print "less than zero     - {}".format(len(delay_sent[delay_sent <= 0]))
    print "zero to 10 minutes - {}".format(len(delay_sent[np.logical_and(delay_sent>0,delay_sent<=10)]))
    print "10 to 20 minutes   - {}".format(len(delay_sent[np.logical_and(delay_sent>10,delay_sent<=20)]))
    print "20 to 30 minutes   - {}".format(len(delay_sent[np.logical_and(delay_sent>20,delay_sent<=30)]))
    print "greater than 30    - {}\n".format(len(delay_sent[delay_sent > 30]))

    #### Get timestamps of releases
    return results
    
def GetStatsEventBasedMonitoring2(event_csv_file):
    #### Get data of monitoring events from csv file
    monitoring_events = pd.read_csv(event_csv_file,parse_dates = ['event_start','event_end'])
    results = monitoring_events.apply(GetReleaseTS2,axis = 1)
    results = monitoring_events.merge(results,left_index = True, right_index = True)
    
    delay_written = np.concatenate(results.delay_written.values)
    delay_sent = np.concatenate(results.delay_sent.values)

    #### Handle nan values
    delay_written = delay_written[delay_written == delay_written]
    delay_sent = delay_sent[delay_sent == delay_sent]
    missed_alerts = np.sum(results.missed_alerts.values)    
    total_release = len(delay_written) + missed_alerts
    
    average_delay_written = np.average(delay_written)
    max_delay_written = max(delay_written)
    min_delay_written = min(delay_written)
    average_delay_sent = np.average(delay_sent)
    sd_delay_written = np.sqrt(np.var(delay_written))
    min_delay_sent = min(delay_sent)
    max_delay_sent = max(delay_sent)
    sd_delay_sent = np.sqrt(np.var(delay_sent))
    
    #### Uncomment to write results to csv file
#    results_to_csv = pd.concat([pd.DataFrame(dict(zip(results.columns,results.ix[i]))) for i in range(len(results))])
#    results_to_csv.to_csv('q2monitoringdata.csv')
    
    print "\n\n"
    print "Event-Based Monitoring Statistics:"
    print "-------------------------------------------\n"
    print "Date Start: {}".format(pd.to_datetime(min(results.event_start)).strftime("%b %d %Y"))
    print "Date End: {}\n".format(pd.to_datetime(max(results.event_end)).strftime("%b %d %Y"))
    print "SMS Written"
    print "Average delay: {} minutes".format(round(average_delay_written,2))
    print "Minimum delay: {} minutes".format(round(min_delay_written,2))
    print "Maximum delay: {} minutes".format(round(max_delay_written,2))
    print "Standard devation: {}\n".format(sd_delay_written)
    print "SMS Sent"
    print "Average delay: {} minutes".format(round(average_delay_sent,2))        
    print "Minimum delay: {} minutes".format(round(min_delay_sent,2))
    print "Maximum delay: {} minutes".format(round(max_delay_sent,2))
    print "Standard devation: {}\n".format(sd_delay_sent)

    print "Total number of EWI releases: {}".format(total_release)
    print "Total number of missed alerts: {}".format(missed_alerts)
    print "Success rate: {}%\n".format(np.round((1 - missed_alerts / float(total_release))*100,2))
    
    print "Frequency Distribution:\n"
    
    print "SMS Written:"
    print "less than zero     - {}".format(len(delay_written[delay_written <= 0]))
    print "zero to 10 minutes - {}".format(len(delay_written[np.logical_and(delay_written>0,delay_written<=10)]))
    print "10 to 20 minutes   - {}".format(len(delay_written[np.logical_and(delay_written>10,delay_written<=20)]))
    print "20 to 30 minutes   - {}".format(len(delay_written[np.logical_and(delay_written>20,delay_written<=30)]))
    print "greater than 30    - {}\n".format(len(delay_written[delay_written > 30]))
    
    print "SMS Sent:"
    print "less than zero     - {}".format(len(delay_sent[delay_sent <= 0]))
    print "zero to 10 minutes - {}".format(len(delay_sent[np.logical_and(delay_sent>0,delay_sent<=10)]))
    print "10 to 20 minutes   - {}".format(len(delay_sent[np.logical_and(delay_sent>10,delay_sent<=20)]))
    print "20 to 30 minutes   - {}".format(len(delay_sent[np.logical_and(delay_sent>20,delay_sent<=30)]))
    print "greater than 30    - {}\n".format(len(delay_sent[delay_sent > 30]))
    
    return results

def GetEWITimeWritten2(ts_release,ts_next_release,site):
    ts_release = pd.to_datetime(ts_release)    
    ts_next_release = pd.to_datetime(ts_next_release)
    if ts_release == ts_next_release:
        ts_end = pd.to_datetime(ts_next_release + pd.Timedelta(2.5,'h'))
    else:
        ts_end = pd.to_datetime(ts_next_release - pd.Timedelta(0.5,'h'))
    ts_release = ts_release - pd.Timedelta(0.5,'h')
    

    
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

def GetReleaseTS2(monitoring_event):
    #### Get datetime of event start and end
    event_start = monitoring_event.event_start
    event_end = monitoring_event.event_end
    event_site = monitoring_event.site
    
    #### Change site name
    event_site = SiteToBgy2(event_site)
    
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
    
    #### Include extended monitoring times
    extended_times = pd.date_range(datetime.datetime.combine(pd.to_datetime(event_end).date() + pd.Timedelta(1,'D'),datetime.time(12,00)),datetime.datetime.combine(pd.to_datetime(event_end).date() + pd.Timedelta(3,'D'),datetime.time(12,00)),freq = '1D')
    release_times = np.insert(extended_times,0,release_times)  
    
    #### Get average delay
    missed_alerts = 0
    
    #### Initialize results list
    delay_written_values = np.array([])
    delay_sent_values = np.array([])
    
    #### Check the delay time
    for i in range(len(release_times)):
        release_time = release_times[i]
        if release_time >= release_times[-4]:
            result = GetEWITimeWritten2(release_time,release_time,event_site)
        else:
            result = GetEWITimeWritten2(release_time,release_times[i+1],event_site)

        if pd.to_datetime(release_time) <= pd.datetime(2017,01,15,20):
            continue

        if type(result) == type('No EWI sent.'):
            missed_alerts += 1
            print "Missed Alert for {} release\n".format(pd.to_datetime(release_time).strftime("%b %d %H:%M"))
            with open('missedalerts.csv','a') as macsv:
                z = pd.DataFrame(columns = ['site','release_time'])
                z.loc[0] = (BgyToSite(event_site),release_time)
                z.to_csv(macsv,header = False,index = False)
            delay_written_values = np.append(delay_written_values,np.nan)
            delay_sent_values = np.append(delay_sent_values,np.nan)
            continue

        ts_written = result[0][0]
        ts_sent = filter(partial(is_not,None),np.array(result[1]))
        
        if pd.to_datetime(release_time).hour == 12:
            cur_delay_written = pd.to_datetime(ts_written) - pd.to_datetime(release_time) - pd.Timedelta(minutes = 5)
        else:
            cur_delay_written = pd.to_datetime(ts_written) - pd.to_datetime(release_time)
        
        delay_sent_values = np.append(delay_sent_values,np.average(map(lambda x:(pd.to_datetime(x) - pd.to_datetime(ts_written))/np.timedelta64(1,'m'),ts_sent)))        
        delay_written_values = np.append(delay_written_values,cur_delay_written/np.timedelta64(1,'m'))

    return pd.Series({'release_times':release_times,'delay_written':delay_written_values,'delay_sent':delay_sent_values,'missed_alerts':missed_alerts})
  
    
    
    
    
    
