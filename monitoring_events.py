from datetime import datetime, timedelta, time
import numpy as np
import pandas as pd

import volatile.memory as mem
import dynadb.db as dbio


def release_time(date_time):
    """Rounds time to 4/8/12 AM/PM.

    Args:
        date_time (datetime): Timestamp to be rounded off. 04:00 to 07:30 is
        rounded off to 8:00, 08:00 to 11:30 to 12:00, etc.

    Returns:
        datetime: Timestamp with time rounded off to 4/8/12 AM/PM.

    """

    time_hour = int(date_time.strftime('%H'))

    quotient = time_hour / 4

    if quotient == 5:
        date_time = datetime.combine(date_time.date()+timedelta(1), time(0,0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1)*4,0))
            
    return date_time


def round_data_ts(date_time):
    """Rounds time to HH:00 or HH:30.

    Args:
        date_time (datetime): Timestamp to be rounded off. Rounds to HH:00
        if before HH:30, else rounds to HH:30.

    Returns:
        datetime: Timestamp with time rounded off to HH:00 or HH:30.

    """

    hour = date_time.hour
    minute = date_time.minute

    if minute < 30:
        minute = 0
    else:
        minute = 30

    date_time = datetime.combine(date_time.date(), time(hour, minute))
    
    return date_time


def get_event_profile(start, end):
    
    query = "SELECT * FROM public_alert_event"
    query += " WHERE event_start BETWEEN '%s'AND '%s'" %(start, end)
    query += " ORDER BY event_id"
    events = dbio.df_read(query)
    
    sites = mem.get('df_sites')
    sites = sites[~sites.active.isnull()].set_index('site_id')
    events = events[events.site_id.isin(sites.index)]
    event_based = events[events.status != 'routine']
    print ('##########')
    print ('%s event-based monitoring with %s invalid(s)' %(len(event_based),
                                    len(events[events.status == 'invalid'])))
        
    event_based['event_duration'] = (event_based['validity'] - event_based['event_start']) / np.timedelta64(1, 'D')
    site_event_prof = event_based.groupby('site_id').agg({'event_duration': ['count', 'max', 'mean']}).reset_index().set_index('site_id')
    site_event_prof.columns = ['count', 'max', 'mean']
    site_event_prof = site_event_prof.join(sites)[['site_code', 'count', 'max', 'mean']]
    print ('##########')
    print ('site event profile:\n %s' %site_event_prof)
    
    return events


def target_release(event_releases, events):
    event_id = event_releases['event_id'].values[0]
    event_start = pd.to_datetime(events[events.event_id == event_id]['event_start'].values[0])
    event_end = pd.to_datetime(events[events.event_id == event_id]['validity'].values[0])
    
    return

def get_web_releases(start, end, events):
    
    query = "SELECT * FROM public_alert_release"
    query += " WHERE data_timestamp BETWEEN '%s'AND '%s'" %(start, end)
    query += " ORDER BY release_id"
    releases = dbio.df_read(query)

    releases['release_time'] = releases['release_time'].apply(lambda x: pd.to_datetime(str(x)[-8:]).time())
    releases['release_timestamp'] = releases['data_timestamp'].apply(lambda x: x.date())
    releases['release_timestamp'] = releases.apply(lambda x: pd.datetime.combine(x['release_timestamp'],x['release_time']),1)
    adjust_releases = releases[releases.data_timestamp > releases.release_timestamp]
    adjust_releases['release_timestamp'] = adjust_releases['release_timestamp'] + timedelta(1)
    releases = releases[releases.data_timestamp <= releases.release_timestamp]
    releases = releases.append(adjust_releases)
    
#    event_releases = releases.groupby('event_id', as_index=False)
#    event_releases.apply(target_release, events=events)
    
    
    return releases

###############################################################################

if __name__ == '__main__':
    
    start = '2018-01-01'
    end = '2018-11-01'
    events = get_event_profile(start, end)
    releases = get_web_releases(start, end, events)
    
#    print ('##### first quarter #####')
#    
#    
#    print ('##### second quarter #####')