from datetime import datetime, timedelta, time
from itertools import product
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
        date_time (datetime): Timestamp with time rounded off to 4/8/12 AM/PM.

    """

    time_hour = int(date_time.strftime('%H'))

    quotient = time_hour / 4

    if quotient == 5:
        date_time = datetime.combine(date_time.date() + timedelta(1), time(0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1)*4))
            
    return date_time


def round_data_ts(date_time):
    """Rounds time to HH:00 or HH:30.

    Args:
        date_time (datetime): Timestamp to be rounded off. Rounds to HH:00
        if before HH:30, else rounds to HH:30.

    Returns:
        date_time (datetime): Timestamp with time rounded off to HH:00 or HH:30.

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
    
    query = "SELECT * FROM sites"
    query += " WHERE active = 1"
    sites = dbio.df_read(query).set_index('site_id')

    events = events[events.site_id.isin(sites.index)]

    event_based = events[events.status != 'routine']
    print ('##########')
    print ('%s event-based monitoring with %s invalid(s)' %(len(event_based),
                                    len(events[events.status == 'invalid'])))

    event_based['event_duration'] = (event_based['validity'] - \
               event_based['event_start']) / np.timedelta64(1, 'D')
    site_event_prof = event_based.groupby('site_id').agg({'event_duration': \
                ['count', 'max', 'mean']}).reset_index().set_index('site_id')
    site_event_prof.columns = ['count', 'max', 'mean']
    site_event_prof = site_event_prof.join(sites)[['site_code', 'count',
                                          'max', 'mean']]
    print ('##########')
    print ('site event profile:\n %s' %site_event_prof)
    
    return events


def get_missed_event_release(event_releases, events):
    event_id = event_releases['event_id'].values[0]
    if event_id in list(events.event_id):
        event_start = pd.to_datetime(events[events.event_id == \
                                            event_id]['event_start'].values[0])
        event_end = pd.to_datetime(events[events.event_id == \
                                          event_id]['validity'].values[0])
        
        if events[events.event_id == event_id]['status'].values[0] != 'routine':
            expected_releases = pd.date_range(start=release_time(event_start),
                                              end=event_end, freq='4H')
            extended_releases = pd.date_range(start=datetime.combine(event_end.date(),
                                                        time(12)) + timedelta(1),
                                                        periods=3, freq='1D')
            expected_releases = expected_releases.append(extended_releases)
            
            missed = sorted(set(expected_releases) - \
                            set(event_releases['target_release']))
            missed_releases = pd.DataFrame({'event_id': [event_id],
                                            'event_start': [event_start],
                                            'missed': [missed],
                                            'event_end': [event_end]})
            
            return missed_releases


def get_expected_routine_release(start, end, events, releases):
    
    routine_sched = pd.DataFrame({'month': [[1, 2, 11, 12], [3, 4], [5],
                                            [6, 7, 8, 9, 10]],
                                  'season1': [[1, 4], [2], [2], [1, 4]],
                                  'season2': [[2], [2], [1, 4], [1, 4]]})
    
    query = "SELECT * FROM sites"
    query += " WHERE active = 1"
    sites = dbio.df_read(query)
    season1_sites = sites[sites.season == 1]['site_id'].values
    season2_sites = sites[sites.season == 2]['site_id'].values
    
    possible_routine = pd.date_range(start=start, end=end, freq='W-TUE')
    possible_routine = possible_routine.append(pd.date_range(start=start,
                                                             end=end,
                                                             freq='W-WED'))
    possible_routine = possible_routine.append(pd.date_range(start=start,
                                                             end=end,
                                                             freq='W-FRI'))
    possible_routine = pd.DataFrame({'date': possible_routine,
                                     'month': possible_routine.month,
                                     'day': possible_routine.dayofweek})
    possible_routine['date'] = possible_routine['date'] + timedelta(0.5)
    
    expected_routine = pd.DataFrame()
    
    for i in range(len(routine_sched)):
        partial_sched = routine_sched[i:i+1]
        month_list = partial_sched['month'].values[0]
        season1_sched = partial_sched['season1'].values[0]
        season2_sched = partial_sched['season2'].values[0]

        season1_dates = possible_routine[(possible_routine.month.isin(month_list)) \
                                       & (possible_routine.day.isin(season1_sched))]['date'].values
        expected_routine = expected_routine.append(pd.DataFrame(list(product(season1_dates,
                                                                             season1_sites)),
                                                   columns=['target_release', 'site_id']))
        season2_dates = possible_routine[(possible_routine.month.isin(month_list)) \
                                       & (possible_routine.day.isin(season2_sched))]['date'].values
        expected_routine = expected_routine.append(pd.DataFrame(list(product(season2_dates,
                                                                             season2_sites)),
                                                   columns=['target_release', 'site_id']))       
    
    event_based = events[events.status != 'routine']
    no_extended_date = event_based[event_based['validity'].apply(lambda x: x.hour).isin([0,4,8])]
    no_extended_date['target_release'] = no_extended_date['validity'].apply(lambda x: pd.datetime.combine(x.date(), time(12)))
    no_extended_date = no_extended_date[['target_release', 'site_id']]
    no_extended_tuple = list(no_extended_date.itertuples(index=False, name=None))
    routine_tuple = list(expected_routine.itertuples(index=False, name=None))
    routine_tuple = set(routine_tuple) - set(no_extended_tuple)
    
    releases = releases.set_index('event_id')
    events = events.set_index('event_id')
    event_monitoring = releases.join(events)
    event_monitoring = event_monitoring[event_monitoring.status != 'routine'][['target_release', 'site_id']]
    event_monitoring['site_id'] = event_monitoring['site_id'].apply(lambda x: int(x))
    event_tuple = list(event_monitoring.itertuples(index=False, name=None))
    routine_tuple = set(routine_tuple) - set(event_tuple)
    
    expected_routine = pd.DataFrame(list(routine_tuple), columns=['target_release', 'site_id'])
    
    return expected_routine

#
#def network_prefix(df):
#   num = df.sim_num
#   network_pref = num.apply(lambda x: x[-9] + x[-8])
#   
#   network_type = []
#   for i in range (0, len(network_pref)):    
#       if network_pref[i] in globe:
#           network_type.append('globe')
#       else: 
#           network_type.append('smart')
#       
#   network_type = np.array(network_type)
#   
#   return network_type



def get_web_releases(start, end, events):
    
    query = "SELECT * FROM public_alert_release"
    query += " WHERE data_timestamp BETWEEN '%s'AND '%s'" %(start, end)
    query += " ORDER BY release_id"
    releases = dbio.df_read(query)
    releases = releases[releases.event_id.isin(events.event_id)]

    releases['release_time'] = releases['release_time'].apply(lambda x: \
                                        pd.to_datetime(str(x)[-8:]).time())
    releases['release_timestamp'] = releases['data_timestamp'].apply(lambda x: \
                                            x.date())
    releases['release_timestamp'] = releases.apply(lambda x: \
            pd.datetime.combine(x['release_timestamp'], x['release_time']), 1)
    mn_releases = releases[releases.data_timestamp > releases.release_timestamp]
    mn_releases['release_timestamp'] = mn_releases['release_timestamp'] + timedelta(1)
    releases = releases[releases.data_timestamp <= releases.release_timestamp]
    releases = releases.append(mn_releases)
    releases = releases.sort_values('release_id', ascending=False)
    releases['target_release'] = releases['data_timestamp'] + timedelta(hours=0.5)
        
    return releases


def get_missed_releases(releases, events):
    event_releases = releases.groupby('event_id', as_index=False)
    missed_releases = event_releases.apply(get_missed_event_release,
                                           events=events)
    
    print 'missed_releases', missed_releases.reset_index()
    
    return missed_releases

###############################################################################

if __name__ == '__main__':
    
    start = '2018-01-01'
    end = '2018-10-31 23:59:59'
    
    events = get_event_profile(start, end)
    releases = get_web_releases(start, end, events)
    
    expected_routine = get_expected_routine_release(start, end, events, releases)
    missed_releases = get_missed_releases(releases, events)