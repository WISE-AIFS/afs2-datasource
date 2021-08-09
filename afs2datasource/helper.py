# Copyright 2019 WISE-PaaS/AFS
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License. 

from abc import ABC, abstractmethod

class Helper(ABC):
  @abstractmethod
  async def connect(self):
    pass
  
  @abstractmethod
  def disconnect(self):
    pass

  @abstractmethod
  async def execute_query(self, querySql):
    pass

  @abstractmethod
  def check_query(self, querySql):
    pass

  @abstractmethod
  def is_table_exist(self, table_name):
    pass

  @abstractmethod
  def is_file_exist(self, table_name, file_name):
    pass

  @abstractmethod
  def create_table(self, table_name, columns):
    pass

  @abstractmethod
  def insert(self, table_name, columns, records, source, destination):
    pass

  @abstractmethod
  async def delete_table(self, table_name):
    pass

  @abstractmethod
  def delete_record(self, table_name, condition, file_name):
    pass
