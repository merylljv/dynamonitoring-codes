# -*- coding: utf-8 -*-
"""
Created on Fri Feb 03 15:12:32 2017

@author: MAJV
"""

import pandas as pd
import requests
import numpy as np
from datetime import datetime, timedelta, time

#import querySenslopeDb as q

def RoundTime(date_time):
    # rounds time to 4/8/12 AM/PM
    time_hour = int(date_time.strftime('%H'))

    quotient = time_hour / 4
    if quotient == 5:
        date_time = datetime.combine(date_time.date() + timedelta(1), time(0,0,0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1)*4,0,0))
            
    return date_time

def release_time(df, ts):
    if df['release_time'].values[0] < time(20, 0):
        df['ts_release'] = df['release_time'].apply(lambda x: datetime.combine(ts.date(), x))
    else:
        df['ts_release'] = df['release_time'].apply(lambda x: datetime.combine(ts.date()-timedelta(1), x))
    return df

def target_time(df, release_ext):
    ts = pd.to_datetime(df['data_timestamp'].values[0]).time()
    if ts >= time(11, 30) and ts < time(12, 0):
        df['ts_target'] = df['data_timestamp'].apply(lambda x: RoundTime(x) + timedelta(hours=(5+release_ext)/60.))
    else:
        df['ts_target'] = df['data_timestamp'].apply(lambda x: RoundTime(x) + timedelta(hours=release_ext/60.))
    return df

def feedback(df, AllReleases):
    ts = pd.to_datetime(df['ts'].values[0])
    ts_mon = ts - timedelta(hours=16.5)
    try:
        mon_end = ts - timedelta(hours=4)
        mon_start = mon_end - timedelta(0.5)
    
        CurrReleases = AllReleases[(AllReleases.data_timestamp >= mon_start)&(AllReleases.data_timestamp < mon_end)]
        
        if ts.time() == time(0, 0):
            CurrReleases['ts_release'] = CurrReleases['release_time'].apply(lambda x: datetime.combine(ts.date()-timedelta(1), x))
        else:
            CurrReleasesTS = CurrReleases.groupby('release_time')
            CurrReleases = CurrReleasesTS.apply(release_time, ts=ts)
        
        CurrSiteMon = len(set(CurrReleases.site_id))
    
        if CurrSiteMon <= 5:
            release_ext = 0
        else:
            release_ext = CurrSiteMon - 5
        
        CurrReleasesDataTS = CurrReleases.groupby('data_timestamp')
        CurrReleases = CurrReleasesDataTS.apply(target_time, release_ext=release_ext)
        CurrReleases['time_diff'] = CurrReleases['ts_release'] - CurrReleases['ts_target']
        CurrReleases['time_diff'] = CurrReleases['time_diff'].apply(lambda x: x / np.timedelta64(1,'D'))
        
        Releases_dict = {ts_mon: {'num_site': CurrSiteMon, 'MT': sorted(set(CurrReleases.reporter_id_mt)), 'CT': sorted(set(CurrReleases.reporter_id_ct)), 'delay_release': np.average(CurrReleases['time_diff'].values) * 24 * 60}}
    except:
        Releases_dict = {ts_mon: 'no monitored sites'}
    
    return Releases_dict

def main(start='', end=''):
    
    if start == '' and end == '':
        ts_now = datetime.now()
        if ts_now.time() >= time(12,0):
            end = datetime.combine(ts_now.date(), time(12, 0))
            start = end
        else:
            end = pd.to_datetime(ts_now.date())
            start = end
    elif start == '' or end == '':
        try:
            start = pd.to_datetime(pd.to_datetime(start).date())
            end = start
        except:
            end = pd.to_datetime(pd.to_datetime(end).date())
            start = end
    else:
        start = pd.to_datetime(pd.to_datetime(start).date())
        end = pd.to_datetime(pd.to_datetime(end).date())
    date_range = pd.date_range(start=start, end=end, freq='12H')
    df = pd.DataFrame({'ts':date_range})
    dfts = df.groupby('ts')
    
    r = requests.get('http://dewslandslide.com/api2/getAllReleases')    
    AllReleases = pd.DataFrame(r.json())
    AllReleases['data_timestamp'] = AllReleases['data_timestamp'].apply(lambda x: pd.to_datetime(x))
    AllReleases['release_time'] = AllReleases['release_time'].apply(lambda x: pd.to_datetime(x).time())
    
    r = requests.get('http://dewslandslide.com/api2/getStaff')    
    StaffID = pd.DataFrame(r.json())
    StaffID['id'] = StaffID['id'].apply(lambda x: int(x))
        
    Releases = dfts.apply(feedback, AllReleases=AllReleases)
    Releases_dict = {}
    for i in range(len(Releases)):
        Releases_dict[Releases[i].keys()[0]] = Releases[i].values()[0]
    
    return Releases_dict
    
if __name__ == '__main__':
    df = main(start = '2017-02-13', end = '2017-02-20')