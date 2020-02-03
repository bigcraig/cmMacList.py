#!/usr/bin/env python
# 2nd Version that takes into account stale mac addresses
# 3rd version , will remmber if dead mac detected on previous poll of cmts
# 4th Version  , can handle connection loss without crashing
import pika
import sys
import base64
import zlib
import gzip
import signal

import StringIO
import json
import os.path
import time
import signal


def extract_element_from_json(obj, path):
    '''
    Extracts an element from a nested dictionary or
    a list of nested dictionaries along a specified path.
    If the input is a dictionary, a list is returned.
    If the input is a list of dictionary, a list of lists is returned.
    obj - list or dict - input dictionary or list of dictionaries
    path - list - list of strings that form the path to the desired element
    '''
    def extract(obj, path, ind, arr):
        '''
            Extracts an element from a nested dictionary
            along a specified path and returns a list.
            obj - dict - input dictionary
            path - list - list of strings that form the JSON path
            ind - int - starting index
            arr - list - output list
        '''
        key = path[ind]
        if ind + 1 < len(path):
            if isinstance(obj, dict):
                if key in obj.keys():
                    extract(obj.get(key), path, ind + 1, arr)
                else:
                    arr.append(None)
            elif isinstance(obj, list):
                if not obj:
                    arr.append(None)
                else:
                    for item in obj:
                        extract(item, path, ind, arr)
            else:
                arr.append(None)
        if ind + 1 == len(path):
            if isinstance(obj, list):
                if not obj:
                    arr.append(None)
                else:
                    for item in obj:
                        arr.append(item.get(key, None))
            elif isinstance(obj, dict):
                arr.append(obj.get(key, None))
            else:
                arr.append(None)
        return arr
    if isinstance(obj, dict):
        return extract(obj, path, 0, [])
    elif isinstance(obj, list):
        outer_arr = []
        for item in obj:
            outer_arr.append(extract(item, path, 0, []))
        return outer_arr

def cmMacDiff(cmts,macArray):
# once a dead mac is detected this method will compare the new mac list 
# with the old to determine who died
        newBrick = False
        newBrickList = []
	filename = cmts +'.current'
        if os.path.isfile(filename):
                previousMacList = [line.rstrip('\n') for line in open(filename)]
                set_previousMacList = set(previousMacList)
                set_macArray = set(macArray)
                
                diffset = set_previousMacList - set_macArray
                listDiffset = list(diffset)
                filename = cmts + '.detected'
                f = open(filename,'w')
                for item in listDiffset:
                        f.write("%s\n" % item)
                f.close

                f = open('detected.bricks', 'a+')
                currentMacList = [line.rstrip('\n') for line in f ]
                f.close()
                for item in listDiffset:
                 if not item in currentMacList:
                     #new brick has been detectedcat lat
                   newBrick = True
                   newBrickList.append(item)


                if newBrick:
                    f = open('detected.bricks', 'a+')
                    g = open('latestDeadModem.txt', 'w')
                    for item  in newBrickList:
                        f.write("%s\n" % item)
                        g.write(" the following address is a candidate for dead modem: %s\n" % item)
                    f.close()
                    g.close()
                    print ('send email')
                   #t3.filesender --to 'rakeshashok;travismohr;vince@gmail.com' --cc brettnewman --attach latestDeadModem.txt  --subject 'Notification DEAD modem' 

                return
# write the file if dead modem detect but no current list exists
              
        with open(filename,'w') as f:
             for item in macArray:
               f.write("%s\n" % item)
        f.close
        return
	

print(' [*] Waiting for NXT messages. To exit press CTRL+C')

def callback(ch, method, properties, body):

      decoded_data=gzip.GzipFile(fileobj=StringIO.StringIO(body)).read()
#      print(" Routing Key %r" % (method.routing_key))
      routerKeyList = str.split(method.routing_key,'.')
      cmtsName = routerKeyList[1]
      filename = cmtsName +'.current'
      parsed = json.loads(decoded_data) 
      for key, value in dict.items(parsed):
#    	  print key, value
          
          if key == "data": 
          
           macData = {"data":value} 
#          print extract_element_from_json(macData,["data","macAddr"]) 
           macArray =  extract_element_from_json(macData,["data","macAddr"])
           statusArray = extract_element_from_json(macData,["data","status"])

#          Mark offline modems
           for i in range(0,len(macArray)):
               if statusArray[i] == 1:
                  macArray[i] = 'OFFLINE'


           if deadMac in macArray:
           #check if cmts already in badcmts list
            if cmtsName in deadCmtsList:
               print ("dead mac already detected in cmts")
               return
            print("DEAD mac found in cmts :  ",cmtsName)
            deadCmtsList.append(cmtsName)
            cmMacDiff(cmtsName,macArray)
            return

           if cmtsName in deadCmtsList:
               deadCmtsList.remove(cmtsName)
               print("dead mac in cmts reset detected")

   
           with open(filename,'w') as f:
             for item in macArray:
                 if item != 'OFFLINE':
                    f.write("%s\n" % item)
             f.close
def signal_handler(signal, frame):
  sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
credentials = pika.PlainCredentials('craig', 'craig')
parameters=pika.ConnectionParameters('10.238.131.199',
                                           5672,
                                          'arrisSales',
                                          credentials)
deadMac ='002040DEAD01'
#deadMac ='7823AEA32D29'

deadCmtsList = []
while True:
    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        channel.exchange_declare(exchange='tenant-out',
                                 exchange_type='topic',
                                 durable=True )
        result = channel.queue_declare('DEAD_DETECT',auto_delete=True)
        queue_name = result.method.queue


        binding_keys = sys.argv[1:]
        if not binding_keys:
                sys.stderr.write("Usage: %s  [binding_key]\n" % sys.argv[0])
                sys.exit(1)

        for binding_key in binding_keys:
            channel.queue_bind(exchange='tenant-out',
                               queue=queue_name,
                               routing_key=binding_key)
    #      print json.dumps(parsed, indent=4, sort_keys=True)
    #      print ("   " )
        channel.basic_consume(callback,
                              queue=queue_name,
                              no_ack=True)

        channel.start_consuming()
    except pika.exceptions.ConnectionClosed:
        print ('lost rabbit connection, attempting reconnect')
        time.sleep(1)
	

