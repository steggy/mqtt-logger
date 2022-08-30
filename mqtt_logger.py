#!/usr/bin/env python3
# MQTT logger
# Author: Steggy
# ver 0.3


import paho.mqtt.client as mqtt
import sys
import os
import logging
import datetime
import dbhelper as DB
 
logfile = 'log_filename.txt'
logging.basicConfig(filename=logfile, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

db1 = DB.dbh('192.168.33.33','mqttu','mqttu123','mqtt')

cwd = os.getcwd()

TOPIC = ""
BROKER="192.168.33.88"


encoding = 'utf-8'
debug = 0




def on_connect(client, userdata, flags, rc):
    #print ("Connected with rc: " + str(rc))
    #client.subscribe(TOPIC+"/#")
    client.subscribe(TOPIC+"#")




def mqttlog(SUB, MSG):
    # Using dbhelper for database inserts
    # Prepare SQL query to INSERT a record into the database.
    sql = "INSERT INTO logs(subscribe, message) VALUES ('%s', '%s')" %(SUB, MSG)
    db1.execute(sql)
	

def on_message(client, userdata, msg):
    #print ("Topic: "+ msg.topic+"\nMessage: "+str(msg.payload))
    #str(b'hello', encoding)
    if debug:
        print ("Topic: " + msg.topic + "\nMessage: " + str(msg.payload, encoding))
        #logging.debug("Topic: "+ msg.topic+" Message: "+str(msg.payload))
    mqttlog(msg.topic,str(msg.payload,encoding))    
	
    
    
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, 1883, 60)


    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n Thanks for Playing\n ")





if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1].lower() == 'd':
            debug = 1
    main()







# EOF
