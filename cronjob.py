from crontab import CronTab
from simulatorlmpl import simulator1,config,stepSize
import json
import requests

my_cron = CronTab(user='azureuser')

def getunitname(unitIDList):
    lst = []
    for id in unitIDList:
        query = {"id":id}
        url = config["api"]["meta"] + '/units?filter={"where":' + json.dumps(query) +'}'
        response = requests.get(url)
        tagmeta = json.loads(response.content)
        if len(tagmeta) == 0:
            url = "https://rmds.bhel.in/exactapi" + '/units?filter={"where":' + json.dumps(query) +'}'
            try:
                response = requests.get(url,timeout=10)
                tagmeta = json.loads(response.content)
            except:
                pass

        print(url)
        if len(tagmeta) >0:
            lst.append(tagmeta[0]["name"])
        else:
            lst.append("")


    return lst


for job in my_cron:
    # print(job.command)
    if "python index.py" in job.command and "Loukik" in job.command:
        print(job)
        my_cron.remove(job)

        my_cron.write()

# unitsIdLiset = ["5f0ff2f892affe3a28ebb1c2","63349b9c749f3c3081c6a472","5df8f5a57e961b7f0bccc7ed",
#                 "5f608d3a10723ca5deaab563","5cef6b84be741bf04dc893a1","629865a541a551428dc18861","5e38fbf6738c06000689b920"] 
unitsIdLiset = ["5f608d3a10723ca5deaab563"]
name = getunitname(unitsIdLiset)
sim = simulator1()
for idx,unitsId in enumerate(unitsIdLiset):
    unitName = name[idx].replace("#"," ").replace(" ","")
    allTags = sim.getTagsFromUnitsId(unitsId)
    for i in range(0,len(allTags),stepSize):
        commandL = f"cd /space/es-master/src/Loukik/qaSimulator/ && unitsId={unitsId} startIdx={i} python index.py 1>/space/es-master/src/Loukik/qaSimulator/logs/{unitName}_log.logs 2>> /space/es-master/src/Loukik/qaSimulator/logs/{unitName}_Errorlog.logs"
        print(commandL)
        job = my_cron.new(command=commandL,comment = unitName)
        job.minute.every(1)
        # job = my_cron.new(command=" ")

my_cron.write()


