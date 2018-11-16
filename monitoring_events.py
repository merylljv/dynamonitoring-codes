from datetime import datetime, timedelta, time
from itertools import product
import numpy as np
import pandas as pd
import re

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


def get_sites():
    query = "SELECT * FROM sites"
    query += " WHERE active = 1"
    sites = dbio.df_read(query).set_index('site_id')
    return sites


def get_events(start, end):
    
    query = "SELECT * FROM public_alert_event"
    query += " WHERE event_start BETWEEN '%s'AND '%s'" %(start, end)
    query += " ORDER BY event_id"
    events = dbio.df_read(query)
    
    sites = get_sites()

    events = events[events.site_id.isin(sites.index)]
    
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
    
    sites = get_sites().reset_index()
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
    event_monitoring = event_monitoring[event_monitoring.status != 'routine']
    event_monitoring = event_monitoring[['target_release', 'site_id']]
    event_monitoring['site_id'] = event_monitoring['site_id'].apply(lambda x: int(x))
    event_tuple = list(event_monitoring.itertuples(index=False, name=None))
    routine_tuple = set(routine_tuple) - set(event_tuple)
    
    expected_routine = pd.DataFrame(list(routine_tuple),
                                    columns=['target_release', 'site_id'])
    
    return expected_routine


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


def get_missed_releases(releases, events, expected_routine):
    for event_id in events[events.status == 'invalid'].event_id:
        events.loc[events.event_id == event_id, 'validity'] = max(releases[releases.event_id == event_id].target_release)
    event_releases = releases.groupby('event_id', as_index=False)
    missed_event_releases = event_releases.apply(get_missed_event_release,
                                                 events=events)
    missed_event_releases = missed_event_releases[missed_event_releases['missed'].apply(lambda x: len(x)) != 0].reset_index(drop=True)
    
    monitoring_releases = releases.set_index('event_id').join(events.set_index('event_id'))
    routine_releases = monitoring_releases[monitoring_releases.status == 'routine']
    routine_releases = routine_releases[['target_release', 'site_id']]
    expected_routine_tuple = list(expected_routine.itertuples(index=False, name=None))
    released_routine_tuple = list(routine_releases.itertuples(index=False, name=None))
    missed_routine_tuple = set(expected_routine_tuple) - set(released_routine_tuple)
    missed_routine_releases = pd.DataFrame(list(missed_routine_tuple),
                                           columns=['target_release', 'site_id'])
    
    return missed_event_releases, missed_routine_releases


def get_expected_extended_releases(events):
    extended_releases = events[events.status == 'finished'][['site_id', 'validity']]
    expected_extended_release = pd.DataFrame()
    for i in range(1,4):
        expected_extended_release = expected_extended_release.append(pd.DataFrame({'site_id': extended_releases['site_id'], 'target_release': extended_releases['validity'].apply(lambda x: pd.datetime.combine((x + timedelta(i)).date(), time(12)))}))
    return expected_extended_release


def get_expected_event_releases(missed_event_releases, events, releases):
    for i in missed_event_releases.index:
        missed_event_releases.loc[missed_event_releases.index == i, 'extended'] = [[np.array(missed_event_releases[missed_event_releases.index == i]['missed'].values[0]) > pd.to_datetime(missed_event_releases[missed_event_releases.index == i]['event_end'].values[0])]]
    missed_event_releases['extended'] = missed_event_releases['extended'].apply(lambda x: x[0])
    missed_event = missed_event_releases.set_index('event_id').join(events.set_index('event_id')[['site_id']])[['site_id', 'missed', 'extended']]
    missed_heightened_releases = pd.DataFrame()
    for i in range(len(missed_event)):
        partial = missed_event[i:i+1]
        missed = np.array(partial['missed'].values[0])
        missed = missed[~partial['extended'].values[0]]
        if len(missed) != 0:
            missed_heightened_releases = missed_heightened_releases.append(pd.DataFrame({'target_release': missed, 'site_id': [partial['site_id'].values[0]] * len(missed), 'event_id': [partial.index.values[0]] * len(missed)}))
    
    monitoring_releases = releases.set_index('event_id').join(events.set_index('event_id'))
    
    successful_event_releases = monitoring_releases[(monitoring_releases.status != 'routine') \
                                                 & (monitoring_releases.validity >= monitoring_releases.target_release)][['target_release', 'site_id']]
    
    expected_event_releases = missed_heightened_releases.append(successful_event_releases,
                                                                ignore_index=True)

    return expected_event_releases


def get_smsoutbox(start, end):
    query =  "SELECT outbox_id, ts_written, ts_sent, site_code, org_name, "
    query += "fullname, sim_num, send_status, sms_msg FROM "
    query += "  (SELECT outbox_id, ts_written, ts_sent, sim_num, "
    query += "  CONCAT(firstname, ' ', lastname) AS fullname, sms_msg, "
    query += "  send_status, user_id FROM "
    query += "    (select * FROM comms_db.smsoutbox_users "
    query += "    WHERE sms_msg regexp 'ang alert level' "
    query += "    ) AS outbox "
    query += "  INNER JOIN "
    query += "    (SELECT * FROM comms_db.smsoutbox_user_status "
    query += "    WHERE send_status >= 5 "
    query += "    AND ts_sent BETWEEN '%s' AND '%s' " %(start, end)
    query += "    ) AS stat "
    query += "  USING (outbox_id) "
    query += "  INNER JOIN "
    query += "    comms_db.user_mobile "
    query += "  USING (mobile_id) "
    query += "  INNER JOIN "
    query += "    comms_db.users "
    query += "  USING (user_id) "
    query += "  ) AS msg "
    query += "INNER JOIN "
    query += "  (SELECT user_id, site_code, org_name FROM "
    query += "    (SELECT * FROM comms_db.user_organization "
    query += "    WHERE org_name in ('lewc', 'blgu', 'mlgu', 'plgu', 'pdrrmc') "
    query += "    ) AS org "
    query += "  INNER JOIN "
    query += "    sites "
    query += "  ON sites.site_id = org.fk_site_id "
    query += "  ) AS site_org "
    query += "USING (user_id) "
    query += "GROUP BY site_code, org_name, sms_msg "
    query += "ORDER BY outbox_id DESC"
    smsoutbox = dbio.df_read(query)
    smsoutbox = smsoutbox[smsoutbox.sms_msg.str.contains('ngayong')]
    smsoutbox['sms_msg'] = smsoutbox.apply(lambda row: row['sms_msg'].replace('(current_date)', pd.to_datetime(row['ts_written']).strftime('%B %d, %Y')), axis=1)
    
    format_index = smsoutbox[smsoutbox.sms_msg.str.contains('\(current_date_time\)')].index
    
    for index in format_index:
        smsoutbox_row = smsoutbox[smsoutbox.index == index]
        ts_date = pd.to_datetime(smsoutbox_row['ts_written'].values[0]).date()
        text = smsoutbox_row['sms_msg'].values[0]
        sub_text = re.findall('(?=[APMN][MN])\w+', text)[-1]
        ts_time = (pd.to_datetime(text[re.search('(?=mamayang)\w+', text).end() + 1: re.search('(?=%s)\w+' %sub_text, text).end()].replace('MN', 'AM').replace('NN', 'PM')) - timedelta(hours=4)).time()
        ts = pd.datetime.combine(ts_date, ts_time).strftime('%B %d, %Y %I:%M %p')
        replaced_text = text.replace('(current_date_time)', ts)
        smsoutbox.loc[smsoutbox.index == index, 'sms_msg'] = replaced_text
    
    return smsoutbox


def get_sms_delay(smsoutbox, start, end):
    smsoutbox['target_release'] = smsoutbox['sms_msg'].apply(lambda x: x[re.search('(?=ngayong)\w+', x).end() + 1: re.search('(?=[APMN][MN])\w+', x).end()])
    smsoutbox['target_release'] = smsoutbox['target_release'].apply(lambda x: x.replace('MN', 'AM').replace('NN', 'PM').replace('2018,', '2018').replace(', PM', ' PM').replace(', AM', 'AM').replace(' :', ':'))    
    
    format_index = smsoutbox[smsoutbox['target_release'].apply(lambda x: len(x) > 30)].index
    year_list = range(pd.to_datetime(start).year, pd.to_datetime(end).year + 1)
    
    for index in format_index:
        target_release = smsoutbox[smsoutbox.index == index]['target_release'].values[0]
        ts_date = pd.to_datetime(target_release[:re.search('(?=%s)\w+' %'|'.join(map(str, year_list)), target_release).end()]).date()
        ts_time = (pd.to_datetime(target_release[re.search('(?=mamayang)\w+', target_release).end() + 1:].replace('MN', 'AM').replace('NN', 'PM')) - timedelta(hours=4)).time()
        ts = pd.datetime.combine(ts_date, ts_time).strftime('%B %d, %Y %I:%M %p')
        smsoutbox.loc[smsoutbox.index == index, 'target_release'] = ts
                     
    smsoutbox['target_release'] = pd.to_datetime(smsoutbox['target_release'])
    smsoutbox['routine_delay'] = (smsoutbox['ts_sent'] - (smsoutbox['target_release'] + timedelta(hours=20/60.))) / np.timedelta64(1, 'm')
    smsoutbox['event_delay'] = (smsoutbox['ts_sent'] - (smsoutbox['target_release'] + timedelta(hours=30/60.))) / np.timedelta64(1, 'm')

    return smsoutbox


###############################################################################

if __name__ == '__main__':
    
    start = '2018-01-01'
    end = '2018-10-31 23:59:59'
    sites = get_sites()
    
    # web releases
    events = get_events(start, end)
    releases = get_web_releases(start, end, events)
    expected_routine_releases = get_expected_routine_release(start, end, events, releases)
    missed_event_releases, missed_routine_releases = get_missed_releases(releases, events, expected_routine_releases)
    expected_event_releases = get_expected_event_releases(missed_event_releases, events, releases)
    expected_extended_releases = get_expected_extended_releases(events)
    
    # EWI
    smsoutbox = get_smsoutbox(start, end)
    smsoutbox = get_sms_delay(smsoutbox, start, end)
    
    
    print ('#################### 2018 monitoring  ####################')
    
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

    print ('#########################')
    print ('########## WEB ##########')
    print ('#########################')
    
    releases['min_delay'] = (releases['release_timestamp'] - releases['target_release']) / np.timedelta64(1, 'm')
    for quarter in range(1,5):
        print ('#################### QUARTER %s ####################' %quarter)
        releaseQ = releases[releases['target_release'].apply(lambda x: x.quarter) == quarter]
        release_routine = releaseQ[releaseQ.internal_alert_level.isin(['A0', 'ND'])]
        delayed = release_routine[release_routine.min_delay > 0]
        print ('successful web release = %s' %(100 - (100. * len(delayed) / len(release_routine))))
        print ('average sending delay = %s' %round(np.mean(delayed['min_delay']), 2))
        print ('maximum sending delay = %s' %round(max(delayed['min_delay']), 2))
        release_event = releaseQ[~releaseQ.internal_alert_level.isin(['A0', 'ND'])]
        delayed = release_event[release_event.min_delay > 0]
        print ('successful web release = %s' %(100 - (100. * len(delayed) / len(release_event))))
        print ('average sending delay = %s' %round(np.mean(delayed['min_delay']), 2))
        print ('maximum sending delay = %s' %round(max(delayed['min_delay']), 2))

    print ('#########################')
    print ('########## EWI ##########')
    print ('#########################')
    
    for quarter in range(1,5):
        print ('#################### QUARTER %s ####################' %quarter)
        smsoutboxQ = smsoutbox[smsoutbox['target_release'].apply(lambda x: x.quarter) == quarter]
        smsoutbox_routine = smsoutboxQ[smsoutboxQ.sms_msg.str.contains('Alert 0')]
        delayed_routine = smsoutbox_routine[(smsoutbox_routine.routine_delay > 0) & (smsoutbox_routine.routine_delay < 60*8)]
        print ('routine EWI SMS successfully sent within 20mins = %s' %(100 - (100. * len(delayed_routine) / len(smsoutbox_routine))))
        print ('average sending delay = %s' %round(np.mean(delayed_routine['routine_delay']), 2))
        print ('maximum sending delay = %s' %round(max(delayed_routine['routine_delay']), 2))
        smsoutbox_event = smsoutboxQ[~smsoutboxQ.sms_msg.str.contains('Alert 0')]
        delayed_event = smsoutbox_event[(smsoutbox_event.event_delay > 0) & (smsoutbox_event.event_delay < 60*8)]
        print ('event EWI SMS successfully sent within 30mins = %s' %(100 - (100. * len(delayed_event) / len(smsoutbox_event))))
        print ('average sending delay = %s' %round(np.mean(delayed_event['routine_delay']), 2))
        print ('maximum sending delay = %s' %round(max(delayed_event['routine_delay']), 2))