import json 
import urllib2 
import base64
import time
import psycopg2
import pendulum

from pandas.io.json import json_normalize
from pandas import DataFrame

#clear connection to postgres
conn = None

conn = psycopg2.connect(host="local", database="nz", user="admin", password="admin")
print("Database Connected")
cur = conn.cursor()
rowcount = cur.rowcount

#Yesterday's Date
tz = pendulum.yesterday("UTC")

back = tz.subtract(days=1)
backfile = back.format("YYYY-MM-DD", formatter="alternative")

ytddate = tz.format("YYYY-MM-DD", formatter="alternative")

print("\nLooking for EventFinda's Events AU data which was created at(UTC time) : " + backfile +"\n")


#look for events created at yesterday from UTC date
def check_count():
    url = 'http://api.eventfinda.com.au/v2/events.json?created_since=%s'%(backfile)
    username = 'admin';
    password = 'admin';
    request = urllib2.Request(url)
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)  

    result = urllib2.urlopen(request)
    data = json.load(result)

    event_count =  data["@attributes"]["count"]
    print("Number of events at " + backfile + " (UTC date) = " + str(event_count))
    
    load_eventfinda(event_count)
    
def download(offset):
    url = 'http://api.eventfinda.com.au/v2/events.json?created_since=%s&rows=10&offset=%d'%(backfile,offset)
    username = 'admin';
    password = 'admin';
    request = urllib2.Request(url)
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)

    result = urllib2.urlopen(request)
    data = json.load(result)

    with open("/home/ec2-user/eventfinda/eventfinda_virtualenv/projects/events/events_data/events_au_%s.json"%(backfile), "a") as fd:
        fd.write(json.dumps(data) + "\n")
    fd.close()
    
def load_eventfinda(event_count):
    
    for offset in range(0,event_count,10):
        print(offset)
        download(offset)
        time.sleep(1)

    print("Download Completed")
        
def read_json():
    try:
        with open("/home/ec2-user/eventfinda/eventfinda_virtualenv/projects/events/events_data/events_au_%s.json"%(backfile), "r") as rd:
            while(1):
                line = rd.readline()
                if(len(line)>0):
                    data_handle(line)
                else:
                    return -1
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
                  
def data_handle(line):
    i = 0
    result = json.loads(line)
    dataframe1 = json_normalize(result)

    events = dataframe1.iloc[0]["events"]

    for events in events:
        
        eid = None
        name = None
        presented_by = None
        username = None
        datetime_start = None
        datetime_end = None
        datetime_summary = None
        timezone = None
        description = None
        category_id = None
        location_id = None
        location_summary = None
        address = None
        session_ids = None
        is_featured = None
        is_free = None
        restriction = None
        url = None
        url_slug = None
        lat = None
        lon = None

        events_dataframe = json_normalize(events)

        eid = int(events_dataframe.iloc[0]["id"])
        name =(events_dataframe.iloc[0]["name"])
        presented_by =(events_dataframe.iloc[0]["presented_by"])
        username = (events_dataframe.iloc[0]["username"])
        datetime_start =(events_dataframe.iloc[0]["datetime_start"])
        datetime_end =(events_dataframe.iloc[0]["datetime_end"])
        datetime_summary =(events_dataframe.iloc[0]["datetime_summary"])
        timezone =(events_dataframe.iloc[0]["timezone"])
        description = (events_dataframe.iloc[0]["description"])
        category_id = int(events_dataframe.iloc[0]["category.id"])
        location_id = int(events_dataframe.iloc[0]["location.id"])
        location_summary = (events_dataframe.iloc[0]["location.summary"])
        address = (events_dataframe.iloc[0]["address"])
        is_featured = str(events_dataframe.iloc[0]["is_featured"])
        is_free = str(events_dataframe.iloc[0]["is_free"])
        restriction = (events_dataframe.iloc[0]["restrictions"])
        url = (events_dataframe.iloc[0]["url"])
        url_slug = (events_dataframe.iloc[0]["url_slug"])
        lat = (events_dataframe.iloc[0]["point.lat"])
        lon = (events_dataframe.iloc[0]["point.lng"])

        session = events_dataframe.iloc[0]["sessions.sessions"]
        session_dataframe = json_normalize(session)
        session_ids = (", ".join ( map(str,session_dataframe["id"].values)))

        insert_events = [eid,name,presented_by,username,datetime_start,datetime_end,datetime_summary,timezone,description,category_id,
                 location_id,location_summary,address,session_ids,is_featured,is_free,restriction,url,url_slug,backfile,lat,lon]
        
        update_events = [eid,name,presented_by,username,datetime_start,datetime_end,datetime_summary,timezone,description,category_id,
                 location_id,location_summary,address,session_ids,is_featured,is_free,restriction,url,url_slug,lat,lon,eid]
        
        cur.execute("""
                        select id from events_au_eventfinda
                        where id = {}
                    """.format(eid))
        result = cur.fetchone()
        print(result)
        if(result != None):
            
            print("Event ID : " + str(eid) + " existing data exist, updating old data")
            cur.execute("""
                UPDATE events_au_eventfinda
                SET
                    id = %s,
                    name = %s,
                    presented_by = %s,
                    username = %s,
                    datetime_start = %s,
                    datetime_end = %s,
                    datetime_summary = %s,
                    timezone = %s,
                    description =%s,
                    category_id =%s,
                    location_id =%s,
                    location_summary = %s,
                    address = %s,
                    session_ids =%s,
                    is_featured =%s,
                    is_free = %s,
                    restrictions = %s,
                    url = %s,
                    url_slug =%s,
                    last_updated = now(),
                    latitude =%s,
                    longitude =%s
                WHERE id = %s
                """,(update_events))
            
            print("Update Completed")
            conn.commit()
            update_events = []
            i+=1
        else:
            print ("Insert")
            cur.execute("""
                        Insert into events_au_eventfinda
                        (
                            id,
                            name,
                            presented_by,
                            username,
                            datetime_start,
                            datetime_end,
                            datetime_summary,
                            timezone,
                            description,
                            category_id,
                            location_id,
                            location_summary,
                            address,
                            session_ids,
                            is_featured,
                            is_free,
                            restrictions,
                            url,
                            url_slug,
                            create_time_utc,
                            latitude,
                            longitude
                        )
                        VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                        """,(insert_events))
            print(" Insert Completed")
            conn.commit()
            insert_events = []
            i+=1
            
    print("Number of rows = " + str(i))
        
check_count()
time.sleep(1)
read_json()
conn.commit()
cur.close()
