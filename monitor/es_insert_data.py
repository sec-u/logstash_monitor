#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# @Time    : 16/11/2 14:45

import json
import time
import logging
from datetime import datetime
from Queue import Queue
from mythread import MyThread
from elasticsearch import Elasticsearch, helpers

logger = logging.getLogger('EsInsert')


class EsInsert(object):
    def __init__(self, queue, es_index_name, doc_type,
                 es_ip_port='localhost:9200', bulk_num=0):
        self.elasticsearch_ip_port = es_ip_port
        # 实例化连接es
        self.es = Elasticsearch(hosts=self.elasticsearch_ip_port)
        self.data_queue = queue
        # es 索引名字
        self.es_index_name = es_index_name
        self.doc_type = doc_type
        self.bulk_num = bulk_num
        # es 数据queue
        self.es_data_queue = Queue(maxsize=3000)

    def one_index(self, data):
        try:
            # 转换为 json
            data_json = json.dumps(data)
            # 获取日期
            date = datetime.now().strftime('%Y.%m.%d')
            # es_index_name 加上日期
            index_name = '%s-%s' % (self.es_index_name, date)
            # 插入数据到es
            self.es.index(index=index_name, doc_type=self.doc_type, body=data_json)
            logger.info('one insert data to elasticsearch: %s' % self.elasticsearch_ip_port)
        except Exception as e:
            logger.error(e)

    def bulk_index(self, data):
        """ bulk 插入数据 """
        try:
            helpers.bulk(self.es, data)
            logger.info('bulk insert data to elasticsearch: %s' % self.elasticsearch_ip_port)
        except Exception as e:
            logger.error(e)

    def bulk_data(self):
        """ 构造要插入的bulk数据 """
        bulk_data = []
        while True:
            data = self.es_data_queue.get()
            # 转换为 json
            data_json = json.dumps(data)
            # 获取日期
            date = datetime.now().strftime('%Y.%m.%d')
            # es_index_name 加上日期
            index_name = '%s-%s' % (self.es_index_name, date)
            action = {
                "_index": index_name,
                "_type": self.doc_type,
                "_source": data_json
            }
            bulk_data.append(action)
            if len(bulk_data) == self.bulk_num:
                break
        return bulk_data

    def bulk_worker(self):
        """ bulk 插入 """
        while True:
            bulk_data = self.bulk_data()
            self.bulk_index(bulk_data)

    def one_worker(self):
        """ 单条 index 插入 """
        while True:
            # 从Queue中获取数据
            data = self.es_data_queue.get()
            # 插入数据到es
            self.one_index(data)

    def data_worker(self):
        """ 数据处理为单个字典 """
        while True:
            # 获取原始数据
            data = self.data_queue.get()

            # 原始数据整理、拆分 放入Queue
            for i in data:

                # 判断列队是否已满
                if not self.es_data_queue.full():
                    self.es_data_queue.put(i)
                else:
                    logger.error('es insert queue is full!')
                    time.sleep(1)

    def run(self):
        thread_data = MyThread(self.data_worker)
        thread_data.start()

        if self.bulk_num == 0:
            # 单条提交数据
            self.one_worker()
        else:
            # bulk 提交数据
            self.bulk_worker()
