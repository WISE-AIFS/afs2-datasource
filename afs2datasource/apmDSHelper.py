import json
import os
import requests as req
from pymongo import MongoClient, errors, DESCENDING, ASCENDING
import pandas as pd
import datetime
import base64
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio

class APMDSHelper():
  def __init__(self, username=None, password=None, apmUrl=None, machineIdList=None, parameterList=None, mongouri=None,timeRange=None):
    if username is None or password is None or apmUrl is None or machineIdList is None or parameterList is None or mongouri is None or timeRange is None:
      dataDir = os.getenv('PAI_DATA_DIR', None)
      if dataDir['type'] == 'apm-firehose':
          self.__login_data = {"userName": dataDir['data']['username'], "password": dataDir['data']['password']}
          self.apm_url = dataDir['data']['apmUrl']
          self.machine_list = dataDir['data']['machineIdList']
          self.parameter_list = dataDir['data']['parameterList']
          self.__mongo_credentials = dataDir['data']['credentials']['uri']
          self.time_range = dataDir['data']['timeRange']
          self.get_machine_detail()
      elif (dataDir == None):
        raise AssertionError(
            "Environment parameters need apm_username={username}, apm_password={password}, apm_url={apmUrl}, machine_id_list={machineIdList}, parameter_list={parameterList}, mongo_uri={mongouri} and time_range={timeRange}".format(
                username=self.__login_data['userName'], password=self.__login_data['password'], apmUrl=self.apm_url, machineIdList=self.machine_list, parameterList=self.parameter_list, mongouri=self.__mongo_credentials, timeRange=self.time_range)
        )
    else:
      self.__login_data = {'userName': username, 'password': base64.b64encode(password.encode('UTF-8')).decode('UTF-8')}
      self.apm_url = apmUrl
      self.machine_list = machineIdList
      self.parameter_list = parameterList
      self.__mongo_credentials = mongouri
      self.time_range = timeRange
      self.get_machine_detail()

  def get_token(self):
    login_url = self.apm_url + '/auth/login'
    accept_header = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    counts = 0
    while counts < 5:
      try:
        login_information = req.post(login_url, data=json.dumps(self.__login_data), headers=accept_header, timeout=3)
        if login_information.status_code is 200:
          return json.loads(login_information.content.decode('UTF-8'))
      except:
        print('login failed')

  def get_machine_detail(self):
    self.machine_content = []
    apm_token = self.get_token()
    get_node_detail_url = self.apm_url + '/topo/node/detail/info'
    header = {'Accept': 'application/json', 'Authorization': 'Bearer ' + apm_token['accessToken']}
    for mid in self.machine_list:
      try:
        machineDetail = req.get(get_node_detail_url, headers=header, params='id='+str(mid))
        if (machineDetail.status_code == 200):
          dtInstance = json.loads(machineDetail.text)['dtInstance']
          self.reorganize_detail(dtInstance)
      except:
        print('get detail dailed')
    if len(self.machine_list) == len(self.machine_content):
      self.connect_mongo()

  def reorganize_detail(self,dtInstance):
    tags = []
    compact = []
    dtFeature = dtInstance['feature']['monitor']
    dtProperty = dtInstance['property']['iotSense']
    device = dtProperty['deviceId'].split('@')[-1]
    for i,e in enumerate(dtFeature):
      for p in self.parameter_list:
        if p == e['name']:
          splitTagList = dtFeature[i]['tag'].split('@')
          tags.append({'name': dtFeature[i]['name'], 'tag': splitTagList[len(splitTagList)-1]})
    for t in range(len(tags)):
      compact.append(device + '\\' + tags[t]['tag'])
    self.machine_content.append({'tags': tags, 'info': dtProperty, 'deviceCompact': compact})

  def connect_mongo(self):
    self._connection = AsyncIOMotorClient(self.__mongo_credentials)
    self._db = self.__mongo_credentials.split('/')[-1]

  def generate_querySql(self):
    timeSql = []
    querySql = []
    for tr in range(len(self.time_range)):
      startTS = datetime.datetime.strptime(self.time_range[tr]['start'], '%Y-%m-%d')
      endTS = datetime.datetime.strptime(self.time_range[tr]['end'], '%Y-%m-%d')
      timeSql.append({'ts': {'$gte': startTS, '$lte': endTS}})
    for mc in self.machine_content:
      for d in range(len(mc['deviceCompact'])):
        querySql.append({'s': mc['info']['groupId'], 't': mc['deviceCompact'][d], '$or': timeSql })
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(self.execute_query('scada_HistRawData', querySql))

  def execute_query(self,collection, query_sql):
    data_container = []
    for sql in range(len(query_sql)):
      data = list(self._connection[self._db][collection].find(query_sql[sql], {'_id':0, 's':0, 't':0}).sort('ts',ASCENDING))
      count_data = len(data)
      print(count_data)
      data = pd.DataFrame(data=data)
      if count_data is not 0:
        data.columns = ['ts', (query_sql[sql]['t'] + '\\' + self.parameter_list[(sql % len(self.parameter_list))])]
        data_container.append(data)
    return self.combine_data(data_container)

  def combine_data(self, container):
    time_stamp_index = None
    for i, e in enumerate(container):
      if "Time_Stamp" in e.columns[1] and "Time_Stamp_ms" not in e.columns[1]:
        self.results = container[i]
        time_stamp_index = i
    for contain in range(len(container)):
      if contain == time_stamp_index:
        continue
      self.results = pd.merge(self.results, container[contain], how='left', on='ts')
    return self.results

  def execute(self):
    return self.generate_querySql()

