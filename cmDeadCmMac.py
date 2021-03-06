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
import datetime
# Script Munber 5
# Script will detect new deadmodems  (ie modems
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
# handle cntrl c
def writeLog(str):
    from datetime import datetime
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    str = str +' ' + dt_string
    f = open('dead.log', 'a+')
    f.write(str + '\n')

    f.close


def signal_handler(signal, frame):
  sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
#define the credentials for the rabbit connection
credentials = pika.PlainCredentials('craig', 'craig')
parameters=pika.ConnectionParameters('10.238.131.199',
                                           5672,
                                          'arrisSales',
                                          credentials)
#deadMac ='002040DEAD01'
deadMac ='7823AEA32D29'
deadCmtsList = []
deadCableMacList = []
deadCableMacCmtsList = []
# logic executed upon receiving messages from rabbitmq
def callback(ch, method, properties, body):
    decoded_data = gzip.GzipFile(fileobj=StringIO.StringIO(body)).read()
    #      print(" Routing Key %r" % (method.routing_key))
    routerKeyList = str.split(method.routing_key, '.')
    cmtsName = routerKeyList[1]
    cmts     = routerKeyList[1]
    messageType = routerKeyList[2]
    print('message type received %s',messageType)

    if messageType == 'CmTopology' :
        parsed = json.loads(decoded_data)
        for key, value in dict.items(parsed):
        #    	  print key, value

            if key == "data":
                macData = {"data": value}
                #          print extract_element_from_json(macData,["data","macAddr"])
                macArray = extract_element_from_json(macData, ["data", "macAddr"])
                statusArray = extract_element_from_json(macData, ["data", "status"])
                if deadMac in macArray:

                    # check if cmts already in badcmts list
                    if cmtsName in deadCmtsList:

                        print("dead mac already detected in cmts")
                        return
                    print("DEAD mac found in cmts :  ", cmtsName)

                    deadCmtsList.append(cmtsName)
                    return


                for i in range(0, len(macArray)):
                     if statusArray[i] == 1:
                        macArray[i] = 'OFFLINE'
                filename = cmtsName + '.current'
                with open(filename, 'w') as f:
                     for item in macArray:
                         if item != 'OFFLINE':
                            f.write("%s\n" % item)
                f.close
                return


                if cmtsName in deadCmtsList:
                        deadCmtsList.remove(cmtsName)
                        print("dead mac in cmts reset detected")
    if messageType == 'CmDsRfFactsCmts' or messageType == 'CmUsRfFactsCmts' :
        print('cm mac message received')
        parsed = json.loads(decoded_data)
        for key, value in dict.items(parsed):

            if key == "data":

                 cableMacData = {"data": value}
                 modemMacArray = extract_element_from_json(cableMacData,["data","mac"])
                 cableMacArray = extract_element_from_json(cableMacData, ["data", "cableMac"])
                 cableMacStatus = extract_element_from_json(cableMacData,["data","status"])
                 deadCableMac  = 'ALIVE'
                 for i in range(0, len(modemMacArray)):
                    print (modemMacArray[i])
                    print (cableMacArray[i]['name'])

                    if modemMacArray[i] == deadMac:
                        deadCableMac = cableMacArray[i]['name']
                        deadCableMacCmts = cableMacArray[i]['name'] + cmtsName
                        if deadCableMacCmts not in deadCableMacCmtsList:
                            # find all offline modems on that cable mac
                            for j in range(0, len(modemMacArray)):
                                if cableMacStatus[j] == 1 and cableMacArray[j] == deadCableMac:
                                    offlineModemsCableMacList.append(cableMacArray[j])

                            # print any offline modems that intersect with current list
                            #file =
                            filename = cmts + '.current'
                            if os.path.isfile(filename):
                                previousMacList = [line.rstrip('\n') for line in open(filename)]
                                set_previousMacList = set(previousMacList)
                                set_offlineModemsCableMacList = set(offlineModemsCableMacList)
                                deadModemCandidates = set_previousMacList.intersection(set_offlineModemsCableMacList)
                            deadCableMacList.append(deadCableMac)
                            deadCableMacCmtsList.append(deadCableMacCmts)
                            print('dead modem detected in cablemac %s',deadCableMac)
                            print('dead modem candidates %s',deadModemCandidates)
                            f = open('DEADCableMac.txt','w')
                            result = str(deadCableMac)
                            result = result + ' in CMTS ' + cmtsName
                            f.write('dead modem detected in cable mac  ' + result + '\n')
                            f.close()
                            # t3.filesender --to 'rakeshashok;travismohr;vince@gmail.com' --cc brettnewman --attach DEADCableMac.txt  --subject 'Notification DEAD modem CableMac'
                            print( 'in CMTS ',cmtsName)



# "main loop which handles loss of connection exception
  #
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
        print ('lost connection')
        writeLog ('lost rabbit connection, attempting reconnect')
        time.sleep(1)