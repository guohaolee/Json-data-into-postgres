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
#On 30th April 0900 NZDT time, which is 29th April 2100, script will run and look for events created on 27th April (UTC)

tz = pendulum.now("UTC")
tz = pendulum.now("UTC")
backfile = tz.subtract(days=2)
createddate = backfile.format("YYYY-MM-DD", formatter="alternative")
modifieddate = backfile.format("YYYY-MM-DD", formatter="alternative")

print("\nLooking for EventFinda's Events AU data which was created and modified on (UTC time) : " + createddate +"\n")


#look for events created at yesterday from UTC date
def check_created():
    url = 'http://api.eventfinda.com.au/v2/events.json?created_since=%s'%(createddate)
    username = 'admin';
    password = 'admin';
    request = urllib2.Request(url)
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)  

    result = urllib2.urlopen(request)
    data = json.load(result)

    event_count =  data["@attributes"]["count"]
    print("Number of events created_at on " + createddate + " (UTC date) = " + str(event_count))
    
    load_created(event_count)
    
def download_created(offset):
    url = 'http://api.eventfinda.com.au/v2/events.json?created_since=%s&rows=10&offset=%d'%(createddate,offset)
    username = 'admin';
    password = 'admin';
    request = urllib2.Request(url)
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)

    result = urllib2.urlopen(request)
    data = json.load(result)

    with open("/home/ec2-user/eventfinda/eventfinda_virtualenv/projects/events/events_data/events_au_created_%s.json"%(createddate), "a") as fd:
        fd.write(json.dumps(data) + "\n")
    fd.close()
    
def load_created(event_count):
    
    for offset in range(0,event_count,10):
        print(offset)
        download_created(offset)
        time.sleep(1)

    print("Download Completed")
        
def read_created():
    try:
        with open("/home/ec2-user/eventfinda/eventfinda_virtualenv/projects/events/events_data/events_au_created_%s.json"%(createddate), "r") as rd:
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
                 location_id,location_summary,address,session_ids,is_featured,is_free,restriction,url,url_slug,createddate,createddate,lat,lon]
        
        update_events = [eid,name,presented_by,username,datetime_start,datetime_end,datetime_summary,timezone,description,category_id,
                 location_id,location_summary,address,session_ids,is_featured,is_free,restriction,url,url_slug,createddate,lat,lon,eid]
        
        cur.execute("""
                        select id from events_au_eventfinda
                        where id = {}
                    """.format(eid))
        result = cur.fetchone()
        if(result != None):
            
            print("\nEvent ID : " + str(eid) + " existing data exist, updating old data")
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
                    last_updated_utc = %s,
                    latitude =%s,
                    longitude =%s
                WHERE id = %s
                """,(update_events))
            
            conn.commit()
            update_events = []
            i+=1
        else:
            print("\nEvent ID : " + str(eid) + " Insert")
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
                            last_updated_utc,
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
                            %s,
                            %s
                        )
                        """,(insert_events))
            conn.commit()
            insert_events = []
            i+=1
            
    print("Number of rows = " + str(i))

def check_modified():
    url = 'http://api.eventfinda.com.au/v2/events.json?modified_since=%s'%(modifieddate)
    username = 'admin';
    password = 'admin';
    request = urllib2.Request(url)
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)

    result = urllib2.urlopen(request)
    data = json.load(result)

    event_count =  data["@attributes"]["count"]
    print("\nNumber of events modified_since on " + modifieddate + " (UTC date) = " + str(event_count))

    load_modified(event_count)


def download_modified(offset):
    url = 'http://api.eventfinda.com.au/v2/events.json?modified_since=%s&rows=10&offset=%d'%(modifieddate,offset)
    username = 'admin';
    password = 'admin';
    request = urllib2.Request(url)
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)

    result = urllib2.urlopen(request)
    data = json.load(result)

    with open("/home/ec2-user/eventfinda/eventfinda_virtualenv/projects/events/events_data/events_au_modified_%s.json"%(modifieddate), "a") as fd:
        fd.write(json.dumps(data) + "\n")
    fd.close()

def load_modified(event_count):

    for offset in range(0,event_count,10):
        print(offset)
        download_modified(offset)
        time.sleep(1)

    print("Download Completed")


def read_modified():
    try:
        with open("/home/ec2-user/eventfinda/eventfinda_virtualenv/projects/events/events_data/events_au_modified_%s.json"%(modifieddate), "r") as rd:
            while(1):
                line = rd.readline()
                if(len(line)>0):
                    data_handle(line)
                else:
                    return -1
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)

def add_regions_local_timestamp():
    print("Truncate events_au_eventfinda_with_regions")
    cur.execute("""Truncate events_au_eventfinda_with_regions""")
    print("Insert Completed")
    print("Number of Rows Deleted : ", cur.rowcount)
    conn.commit()

    print("Load into events_au_eventfinda_with_regions")
    cur.execute( """
                    insert into events_au_eventfinda_with_regions
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
                        last_updated_utc,
                        latitude,
                        longitude,
                        tourism_region_name,
                        state,
                        geom
                    )
                    SELECT
                    
                    	    a.id,
                    	    a.name,
                    	    a.presented_by,
                    	    a.username,
                    	    a.datetime_start,
                    	    a.datetime_end,
                    	    a.datetime_summary,
                    	    a.timezone,
                    	    a.description,
                    	    a.category_id,
                    	    a.location_id,
                    	    a.location_summary,
                    	    a.address,
                    	    a.session_ids,
                    	    a.is_featured,
                    	    a.is_free,
                    	    a.restrictions,
                    	    a.url,
                    	    a.url_slug,
                    	    a.create_time_utc,
                    	    a.last_updated_utc,
                    	    a.latitude,
                    	    a.longitude,
                    	    b.tourism_region_name,
                    	    b.state,
                    	    ST_SetSRID(ST_MakePoint(a.longitude,a.latitude),4326)
                    from events_au_eventfinda a, australian_tourism_regions b
                    WHERE st_contains(b.geom,ST_SetSRID(ST_MakePoint(a.longitude,a.latitude),4326));""")
    print("Insert Completed")
    print("Number of Rows Updated : ", cur.rowcount)
    conn.commit()

    print("Update local_datetime and update_datetime")
    cur.execute(""" 
                    Update events_au_eventfinda_with_regions
                    set create_time_local = case
                        when state = 'New South Wales'                then create_time_utc + INTERVAL '11 hours'
                        when state = 'Queensland'                     then create_time_utc + INTERVAL '10 hours'
                        when state = 'South Australia'                then create_time_utc + INTERVAL '10 hours 30 minutes'
                        when state = 'Western Australia'              then create_time_utc + INTERVAL '8 hours'
                        when state = 'Victoria'                       then create_time_utc + INTERVAL '11 hours'
                        when state = 'Northern Territory'             then create_time_utc + INTERVAL '9 hours 30 minutes'
                        when state = 'Australian Capital Territory'   then create_time_utc + INTERVAL '11 hours'
                        when state = 'Tasmania'                       then create_time_utc + INTERVAL '11 hours'
                    else create_time_local + INTERVAL '12 hours'
                    end;""")
    print("Update Completed")
    print("Number of Rows Updated : ", cur.rowcount)
    conn.commit()

    print("Update local_datetime and update_datetime")
    cur.execute("""
                    Update events_au_eventfinda_with_regions
                    set last_updated_local = case
                        when state = 'New South Wales'                then last_updated_utc + INTERVAL '11 hours'
                        when state = 'Queensland'                     then last_updated_utc + INTERVAL '10 hours'
                        when state = 'South Australia'                then last_updated_utc + INTERVAL '10 hours 30 minutes'
                        when state = 'Western Australia'              then last_updated_utc + INTERVAL '8 hours'
                        when state = 'Victoria'                       then last_updated_utc + INTERVAL '11 hours'
                        when state = 'Northern Territory'             then last_updated_utc + INTERVAL '9 hours 30 minutes'
                        when state = 'Australian Capital Territory'   then last_updated_utc + INTERVAL '11 hours'
                        when state = 'Tasmania'                       then last_updated_utc + INTERVAL '11 hours'
                    else last_updated_local + INTERVAL '12 hours'
                    end;""")
    print("Update Completed")
    print("Number of Rows Updated : ", cur.rowcount)
    conn.commit()


                    
if __name__ == "__main__":
    check_created()
    time.sleep(1)
    read_created()
    conn.commit()

    check_modified()
    time.sleep(1)
    read_modified()
    conn.commit()

    add_regions_local_timestamp()

    cur.close()
