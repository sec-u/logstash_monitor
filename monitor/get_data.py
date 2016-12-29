#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# @Time    : 16/11/2 14:17


import time
import logging
from elasticsearch import Elasticsearch

logger = logging.getLogger('LogstashMonitor')


class LogstashMonitor(object):
    def __init__(self, hosts, index, queue, count_time=300, intervals=180):
        self.hosts = hosts
        self.index = index
        self.count_time = count_time
        self.es_queue = queue
        self.intervals = intervals
        self.es = Elasticsearch(hosts=self.hosts, timeout=30)

    def get_data(self):
        """ 构建数据 """
        count_time = time.strftime('%Mm', time.gmtime(self.count_time))
        body = ('{"fields":["t_Log_type","t_host_ip"],"query":{"filtered":{"query":{"query_string":{"query":"*"}},'
                '"filter":{"bool":{"must":{"range":{"@timestamp":{"from":"now-%s"}}}}}}},"size":0,"aggs":{"types"'
                ':{"terms":{"field":"t_Log_type","size":200},"aggs":{"errortypes":{"terms":{"field":"t_host_ip",'
                '"size":200}}}}}}') % count_time

        index = '%s%s' % (self.index, '-*')
        try:
            raw_data = self.es.search(index=index, body=body)

            data = raw_data['aggregations']['types']['buckets']

        except Exception as e:
            logger.error(e, self.hosts, index)

            data = None

        return data

    def run(self):
        while True:
            t0 = time.clock()

            data = self.get_data()

            if data and not self.es_queue.full():
                self.es_queue.put(data)

            # 执行间隔时间
            t = time.clock() - t0
            # 睡眠时间减去执行时间 保证间隔时间相等
            sleep_time = self.intervals - t
            if sleep_time < 0:
                sleep_time = 0

            time.sleep(sleep_time)
