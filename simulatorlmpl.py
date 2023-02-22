import requests
import json
import numpy as np
import pandas as pd
import platform
import logging as lg
import time
import traceback
import sys
import threading
import grequests

stepSize = 5000

version = platform.python_version().split(".")[0]
if version == "3":
  import app_config.app_config as cfg
elif version == "2":
  import app_config as cfg
config = cfg.getconfig()

class simulatorV2:
    def getTagsFromUnitsId(self,unitsId):
        query = {"unitsId":unitsId}
        url = config["api"]["meta"] + '/tagmeta?filter={"where":' + json.dumps(query) +',"fields":["dataTagId","calculationId"]}'
        response = requests.get(url)
        lst = self.getEffMap(unitsId)
        tagmeta = json.loads(response.content)
        allTags = []
        
        if len(tagmeta) == 0 and response.status_code == 200:
            url = "https://rmds.bhel.in/exactapi" + '/tagmeta?filter={"where":' + json.dumps(query) +',"fields":["dataTagId","calculationId"]}'
            print(url)
            try:
                response = requests.get(url,timeout=10)
                tagmeta = json.loads(response.content)
            except:
                pass
            
        for tag in tagmeta:
            if "calculationId" not in list(tag.keys()) and tag["dataTagId"] not in lst:
                allTags.append(tag["dataTagId"])
        
        return allTags


    def getEffMap(self,unitsId):
        url = "http://13.251.5.125/exactapi/units/"+unitsId+"/boilerStressProfiles?filter=%7B%22where%22%3A%7B%22type%22%3A%22efficiencyMapping%22%7D%7D"
        response = requests.get(url)
        lst = []
        try:
            effMap  = json.loads(response.content)[0]
            for key,value in effMap["output"].items():
                if key != "type":
                    for val in value:
                        if type(val) == dict:
                            lst += list(val["outputs"].values())
        except:
            pass
        return lst


class simulator1():
    def getLastValues(self,taglist,end_absolute=0):
        # print("tags passed: "+str(len(taglist)))
        if end_absolute !=0:
            query = {"metrics": [],"start_absolute": 1, end_absolute: end_absolute}
        else:
            query = {"metrics": [],"start_absolute":1}
        for tag in taglist:
            query["metrics"].append({"name": tag,"order":"desc","limit":1})
        # print(query)
        try:
            res = requests.post(config['api']['query'],json=query).json()
            
            df = pd.DataFrame()
            for tag in res["queries"]:
                try:
                    df.loc[0,"time_" + tag["results"][0]["name"]] =  tag["results"][0]["values"][0][0]
                    df.loc[0,tag["results"][0]["name"]] = tag["results"][0]["values"][0][1]
                except: 
                    pass
        
        except Exception as e:
            print(traceback.format_exc())
            return pd.DataFrame()
        return df 


    def getLastValuesV2(self,taglist,end_absolute=0):
        print("tags passed: "+str(len(taglist)))
        startTime = time.time()
        if end_absolute !=0:
            query = {"metrics": [],"start_absolute": 1, end_absolute: end_absolute}
        else:
            query = {"metrics": [],"start_absolute":1}
        
        param1 = []
        urls = []
        for i in range(0,len(taglist),100):
            for tag in taglist[i:i+100]:
                query["metrics"].append({"name": tag,"order":"desc","limit":1})

            param1.append(query.copy())
            urls.append(config['api']['query'])
                
            # print(query)
            query["metrics"] = []
            # print(param)

        # print(param1)

        rs = (grequests.post(u, json=param1[idx]) for idx,u in enumerate(urls))
        df = pd.DataFrame(index = [0])
        requests = grequests.map(rs)
        for res1 in requests:
            if res1.status_code == 200:
                res = res1.json()
                for tag in res["queries"]:
                    try:
                        df.loc[0,"time_" + tag["results"][0]["name"]] =  tag["results"][0]["values"][0][0]
                        df.loc[0,tag["results"][0]["name"]] = tag["results"][0]["values"][0][1]
                    except: 
                        pass
        print(time.time() - startTime)    
        return df

                
    def getEffMap(self,unitsId):
        url = "http://13.251.5.125/exactapi/units/"+unitsId+"/boilerStressProfiles?filter=%7B%22where%22%3A%7B%22type%22%3A%22efficiencyMapping%22%7D%7D"
        response = requests.get(url)
        lst = []
        try:
            effMap  = json.loads(response.content)[0]
            for key,value in effMap["output"].items():
                if key != "type":
                    for val in value:
                        if type(val) == dict:
                            lst += list(val["outputs"].values())
        except:
            pass
        return lst
            
        

    def getTagsFromUnitsId(self,unitsId):
        query = {"unitsId":unitsId}
        url = config["api"]["meta"] + '/tagmeta?filter={"where":' + json.dumps(query) +',"fields":["dataTagId","calculationId"]}'
        response = requests.get(url)
        lst = self.getEffMap(unitsId)
        tagmeta = json.loads(response.content)
        allTags = []
        
        if len(tagmeta) == 0:
            url = "https://rmds.bhel.in/exactapi" + '/tagmeta?filter={"where":' + json.dumps(query) +',"fields":["dataTagId","calculationId"]}'
            print(url)
            try:
                response = requests.get(url,timeout=10)
                tagmeta = json.loads(response.content)
            except:
                pass
            # print(response)
            # lst = self.getEffMap(unitsId)
            tagmeta = json.loads(response.content)

        for tag in tagmeta:
            if "calculationId" not in list(tag.keys()) and tag["dataTagId"] not in lst:
                allTags.append(tag["dataTagId"])
        
        return allTags

    
    def liveDataUpload(self,allTags,unitsId,client,index):
        startTime = time.time()*1000
        
        tagList = allTags[index[0]:index[1]]
        # print(df)
        for dataTagId in tagList:
            df = self.getLastValues([dataTagId])
            topicLine = "u/" + unitsId + "/" + dataTagId + "/r"
            # print(topicLine)
            try:
                value = df.loc[0,dataTagId]
                postBody = {
                    "v":float(value),
                    "t" : startTime
                }
                client.publish(topicLine,json.dumps(postBody))
                
            except KeyError:
                pass
        print(time.time()*1000 - startTime,index)


    def getValuesV2(self,tagList):
        url = config["api"]["query"]
        metrics = []
        for tag in tagList:
            tagDict = {
                    "tags": {},
                    "name": tag,
                    "limit": "1",
                    "order":"desc"
            }

            metrics.append(tagDict)
            
        query ={
            "metrics":metrics,
            "plugins": [],
            "cache_time": 0,
            "start_relative": {
                "value": "100",
                "unit": "years"
                }
            }
            
    #     print(json.dumps(query,indent=4))
        res=requests.post(url=url, json=query)
        values=json.loads(res.content)
        finalDF = pd.DataFrame()
        for i in values["queries"]:
    #         print(json.dumps(i["results"][0]["name"],indent=4))
            df = pd.DataFrame(i["results"][0]["values"],columns=["time",i["results"][0]["name"]])
    #         display(df)
    #         print("-"*100)
            try:
                finalDF = pd.concat([finalDF,df.set_index("time")],axis=1)
            except Exception as e:
                print(e)
                finalDF = pd.concat([finalDF,df],axis=1)
            
        finalDF.reset_index(inplace=True)
        print()
        return finalDF