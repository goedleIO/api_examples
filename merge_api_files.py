'''Gets all raw data for an App key, from the GIO infrastructure for a certain timespan
Usage:
    merge_api_files.py <app_key> <master_key> <start_date> <number_of_days>
'''
from docopt import docopt
import requests
import bz2
import datetime as dt
from datetime import datetime
import json
import operator
from collections import Counter


def merge_data(start_date, number_of_days, app_key, master_key):
    headers = {"x-goedle-app-key": app_key, "x-goedle-master-key": master_key}
    data_rows = []
    events = set()
    users = set()
    event_counts = Counter()
    users_counts_event = Counter()
    start_data_file_path = datetime.strptime(start_date, '%Y/%m/%d').strftime("%Y-%m-%d")
    for n in range(number_of_days):
        single_date = datetime.strptime(start_date, '%Y/%m/%d') + dt.timedelta(n)
        url = 'http://api.goedle.io/apps/%s/data/' % app_key
        single_date_url = single_date.strftime("%Y/%m/%d")
        # statistics
        url = url+single_date_url
        r = requests.get(url, headers=headers, allow_redirects=True)
        temp_file = bz2.decompress(r.content).decode("utf-8")
        raw_rows = []
        for line in temp_file.splitlines():
            try:
                # Check if we ha
                row = json.loads(line)
                raw_rows.append(json.dumps(row))
                # This is for statistics
                users_counts_event[row['user_id']] += 1
                event_counts[row['event']] += 1
                users.add(row['user_id'])
                events.add(row['event'])
            except Exception as e:
                print (e)
                print ("error in: %s" % line)
        data_rows.extend(raw_rows)
        print ("%s Events on date %s" % (len(raw_rows), single_date_url))
    merged = "\n".join(data_rows)
    print ("\n")
    print ("%s events from %s days" % (len(data_rows), number_of_days))
    print ("%s unique users were observed") % len(users)
    print ("%s unique events were observed") % len(events)
    print ("\n")
    print ("Overall Events counts")
    sorted_events = sorted(event_counts.items(), key=operator.itemgetter(1))
    print ("Event: Number of Occurrences")
    for event in sorted_events:
        print ("%s: %s" % (event[0], event[1]))
    file_name = "/tmp/%s_%s_%s_days.json" % (app_key, start_data_file_path, number_of_days)
    with open(file_name, "w") as text_file:
        text_file.write(merged)
    print ('\n')
    print ("Merged data is stored in %s") % file_name

if __name__ == '__main__':
    '''
    To get the Master Key and App Key, please contact support@goedle.io
    start_date -- Start date, from where you are getting data
    number_of_days -- Defines the days  beginning from the start date from where you get data
    app_key -- An identifier of the App, Virtual Lab, Game
    master_key -- The Master Key is needed for authorization at the goedle.io API 
    '''
    arguments = docopt(__doc__)
    print(arguments)
    if arguments['<start_date>']:
        try:
            start_date = arguments['<start_date>']
        except Exception as e:
            print ("No valid start date use -> YYYY/MM/DD")
    else:
        print ('No valid start  date')
    if arguments['<number_of_days>']:
        try:
            number_of_days = int(arguments['<number_of_days>'])
        except Exception as e:
            print ("number of days have to be an int")
    else:
        print ('No number of days default is 1')
        number_of_days = 1
    if arguments['<app_key>']:
        app_key = arguments['<app_key>']
    else:
        print ('App key missing')
    if arguments['<master_key>']:
        master_key = arguments['<master_key>']
    else:
        print ('Master key is missing')
    merge_data(start_date, number_of_days, app_key, master_key)
