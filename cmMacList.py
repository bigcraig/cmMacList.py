#!/usr/bin/env python
import pika
import sys
import base64
import zlib
import gzip
import signal

import StringIO
import json
import os.path
credentials = pika.PlainCredentials('craig', 'craig')
parameters=pika.ConnectionParameters('10.238.131.199',
                                           5672,
                                          'arrisSales',
                                          credentials)
deadMac ='002040DEAD01'
#deadMac ='7823AEA32D81'

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
                f =  open('detected.bricks','a+')
                currentMacList = [line.rstrip('\n') for line in f ]
                for item in listDiffset:
                 if not item in currentMacList:
                     #new brick has been detected
                   newBrick = True
                   f.write("%s\n" % item)  
                f.close
                if newBrick == True:
                   file = open('latestDeadModem.txt','w')
                   file.write('The following MAC address may have become a dead modem : ')
                   file.write("%s\n" % item)
                   file.close
                   print ('send email') 
                   #t3.filesender --to 'rakeshashok;travismohr;vince@gmail.com' --cc brettnewman --attach latestDeadModem.txt  --subject 'Notification DEAD modem' 
                print diffset
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
           if deadMac in macArray:
           	print("DEAD mac found in cmts :  ",cmtsName)
                cmMacDiff(cmtsName,macArray)
                return	   
   
           with open(filename,'w') as f:
             for item in macArray:
               f.write("%s\n" % item)
             f.close


	
#      print json.dumps(parsed, indent=4, sort_keys=True)  
#      print ("   " )  
channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()

