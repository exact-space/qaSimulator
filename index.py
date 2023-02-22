from simulatorlmpl import simulatorV2,config,stepSize
import os
import paho.mqtt.client as paho
from apscheduler.schedulers.background import BackgroundScheduler
import threading
import time
import json


unitsId = os.environ.get("unitsId") if os.environ.get("unitsId")!=None else None

unitsId = "5f608d3a10723ca5deaab563"
config['BROKER_ADDRESS_QA'] = "20.228.168.6"


if unitsId==None:
    print("no unit id passed")
    exit()

doneTags = []
sim = simulatorV2()
allTags = sim.getTagsFromUnitsId(unitsId)
# print(allTags)

def on_message_prod(client, userdata, msg):
    # print(msg.topic)
    dataTagId = msg.topic.split("/")[2]

    body = json.loads(msg.payload)
    # print(body)
    publishBody = {}
    publishBody["t"] = body[0]["t"]
    try:
        publishBody["v"] = float(body[0]["v"])
    except:
        publishBody["v"] = float(body[0]["r"])

    # print(publishBody)
    client_qa.publish(msg.topic,json.dumps(publishBody))
    doneTags.append(dataTagId)


def on_connect_prod(client, userdata, flags, rc):
    print("connected to prod")
    # for tag in allTags:
    #     topicLine = "u/" + unitsId + "/" + tag + "/r"
    #     client.subscribe(topicLine)

def on_log_prod(client, userdata, obj, buff):
    print("prod_log:" + str(buff))

port = os.environ.get("Q_PORT")
if not port:
    port = 1883
else:
    port = int(port)
print("Running port", port)

client_prod= paho.Client()
client_prod.on_log = on_log_prod
client_prod.on_connect = on_connect_prod
client_prod.on_message = on_message_prod

client_prod.connect(config['BROKER_ADDRESS'], port, 60)
        

def on_log_qa(client, userdata, obj, buff):
    print("qa_log:" + str(buff))

def on_connect_qa(client, userdata, flags, rc):
    print("conected to qa")
    for tag in allTags:
        topicLine = "u/" + unitsId + "/" + tag + "/r"
        # print(topicLine)
        client.subscribe(topicLine)

def on_message_qa(client, userdata, msg):
    # print(msg.topic)
    body = json.loads(msg.payload)
    print(body)

client_qa = paho.Client()
client_qa.on_log = on_log_qa
client_qa.on_connect = on_connect_qa
client_qa.on_message = on_message_qa

client_qa.connect(config['BROKER_ADDRESS_QA'], port, 60)



client_prod.loop_forever(retry_first_connection=True)
client_qa.loop_forever(retry_first_connection=True)