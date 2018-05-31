import time
import json
import urllib2
import base64
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

print("\nLooking for EventFinda's Events NZ data which was created and modified on (UTC time) : " + createddate +"\n")

#look for events created at yesterday from UTC date
def check_created():
    url = 'http://api.eventfinda.co.nz/v2/locations.json?created_since=%s'%(createddate)
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
    url = 'http://api.eventfinda.co.nz/v2/locations.json?created_since=%s&rows=10&offset=%d'%(createddate,offset)
    username = 'admin';
    password = 'admin';
    request = urllib2.Request(url)
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)

    result = urllib2.urlopen(request)
    data = json.load(result)

    with open("/home/ec2-user/eventfinda/eventfinda_virtualenv/projects/location/location_data/locations_nz_created_%s.json"%(createddate), "a") as fd:
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
        with open("/home/ec2-user/eventfinda/eventfinda_virtualenv/projects/location/location_data/locations_nz_created_%s.json"%(createddate), "r") as fd:
            while(1):
                line = fd.readline()
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
    
    locations = dataframe1.iloc[0]["locations"]
    
    for data in locations:
        lid = None
        name = None
        address = None
        location_name = None
        location_address = None
        description = None
        is_venue = None
        latitude = None
        longitude = None
        
        location_dataframe = json_normalize(data)
        
        lid = int(location_dataframe.iloc[0]["id"])
        name = (location_dataframe.iloc[0]["name"])
        location_name = (location_dataframe.iloc[0]["summary"]).split(",")[0]
        address = (location_dataframe.iloc[0]["summary"])
        if (len(address.split(",")) > 1):
            location_address = address.split(",")[1]
        else:
            location_address = ""
        if (len(address.split(",")) > 2):
             state = address.split(",")[2]
        else:
             state = ""
       
        try:
            description = (location_dataframe.iloc[0]["description"])
        except KeyError:
            description = None
            
        is_venue = str(location_dataframe.iloc[0]["is_venue"])
        
        try:
            latitude =(location_dataframe.iloc[0]["point.lat"])
        except KeyError:
            latitude = 0
        
        try:
            longitude =(location_dataframe.iloc[0]["point.lng"])
        except KeyError:
            longitude = 0
 
        insert_location = [lid,location_name,location_address,description,name,is_venue,latitude,longitude,createddate,createddate]
        
        update_location = [lid,location_name,location_address,description,name,is_venue,latitude,longitude,createddate,lid]

        cur.execute("""
                        select id from location_nz_eventfinda
                        where id = {}
                    """.format(lid))
        result = cur.fetchone()
 
        if (result != None): 
            print("Location ID : " + str(lid) + " existing data exist, updating old data")
            cur.execute("""
                        UPDATE location_nz_eventfinda
                        SET
                            id = %s,
                            location_name = %s,
                            location_address = %s,
                            description = %s,
                            name = %s,
                            is_venue = %s,
                            lat = %s,
                            lng = %s,
                            last_updated_utc = %s
                        WHERE id = %s
                        """,(update_location))
            conn.commit()
            update_location = []
            i+=1
        else:
            print("Location ID : " + str(lid) + " Insert")

            cur.execute("""
                            Insert into location_nz_eventfinda
                            (   
                                id,
                                location_name,
                                location_address,
                                description,
                                name,
                                is_venue,
                                lat,
                                lng,
                                created_time_utc,
                                last_updated_utc
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
                            %s
                        )""",(insert_location))
            conn.commit()
            insert_location = []
            i+=1

    print("Number of rows = " + str(i))

def check_modified():
    url = 'http://api.eventfinda.co.nz/v2/locations.json?modified_since=%s'%(modifieddate)
    username = 'admin';
    password = 'admin';
    request = urllib2.Request(url)
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)

    result = urllib2.urlopen(request)
    data = json.load(result)

    event_count =  data["@attributes"]["count"]
    print("Number of events modified_since on " + modifieddate + " (UTC date) = " + str(event_count))

    load_modified(event_count)

def download_modified(offset):
    url = 'http://api.eventfinda.co.nz/v2/locations.json?modified_since=%s&rows=10&offset=%d'%(modifieddate,offset)
    username = 'admin';
    password = 'admin';
    request = urllib2.Request(url)
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)

    result = urllib2.urlopen(request)
    data = json.load(result)

    with open("/home/ec2-user/eventfinda/eventfinda_virtualenv/projects/location/location_data/locations_nz_modified_%s.json"%(modifieddate), "a") as fd:
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
        with open("/home/ec2-user/eventfinda/eventfinda_virtualenv/projects/location/location_data/locations_nz_modified_%s.json"%(modifieddate), "r") as fd:
            while(1):
                line = fd.readline()
                if(len(line)>0):
                    data_handle(line)
                else:
                    return -1
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)


if __name__ == "__main__":
    check_created()
    time.sleep(1)
    read_created()
    conn.commit()
    
    check_modified()
    time.sleep(1)
    read_modified()
    conn.commit()

    cur.close()
