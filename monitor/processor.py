#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# @Time    : 16/11/2 14:46

import logging
from datetime import datetime

logger = logging.getLogger('Processor')


class Processor(object):
    def __init__(self, raw_data_queue, processing_data_queue, compare_data, command_queue):
        self.raw_data_queue = raw_data_queue
        self.processing_data_queue = processing_data_queue
        self.compare_data = compare_data
        self.command_queue = command_queue

    def get_data(self):
        """ 获取数据,并处理为dict key --> ip_list"""
        data = {}
        raw_data = self.raw_data_queue.get()

        for i in raw_data:
            key = i['key'].lower()

            value = [i['key'] for i in i['errortypes']['buckets']]

            data[key] = value

        return data

    def compare(self):
        """比对数据, 得到不存在的ip list  [{ip:ip, name:key, datetime}, {ip:ip2, name:key, datetime}]"""
        data = self.get_data()

        problematic_data = []

        date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')

        for key in self.compare_data.keys():
            # 最新的ip列表
            data_ip = data.get(key)
            # 初始化ip列表
            compare_ip = self.compare_data[key]

            # 检查KEY 有问题返回空列表
            if not data_ip:
                logger.error('key error "%s" !!!' % key)
                data_ip = []

            # 得到比对后的ip
            problematic_ip = [ip for ip in compare_ip if ip not in data_ip]

            for ip in problematic_ip:
                d = dict(ip=ip, name=key)
                d['@timestamp'] = date
                problematic_data.append(d)

        return problematic_data

    def run(self):
        while True:
            problematic_data = self.compare()

            # ES 数据插入队列
            if len(problematic_data) > 0 and not self.processing_data_queue.full():
                self.processing_data_queue.put(problematic_data)

            # 命令执行队列
            if len(problematic_data) > 0 and not self.command_queue.full():
                self.command_queue.put(problematic_data)
