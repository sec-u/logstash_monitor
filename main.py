#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# @Time    : 16/11/2 14:43


import json
import logging
from Queue import Queue
from ConfigParser import ConfigParser
from monitor.mythread import MyThread
from monitor.get_data import LogstashMonitor
from monitor.processor import Processor
from monitor.es_insert_data import EsInsert
from monitor.command import Command

logging.basicConfig(level=logging.INFO,
                    format=('[%(asctime)s] [%(levelname)s] [%(pathname)s] [%(funcName)s]'
                            ' [line:%(lineno)d] %(message)s'),
                    )

if __name__ == '__main__':
    cf = ConfigParser()
    cf.read('conf')

    # 采集参数
    collection_count_time = cf.getint('collection', 'count_time')
    collection_intervals = cf.getint('collection', 'intervals')
    collection_index_name = cf.get('collection', 'index')
    collection_hosts = cf.get('collection', 'hosts').split(',')

    # 数据插入参数
    es_hosts = cf.get('elasticsearch', 'hosts').split(',')
    es_index_name = cf.get('elasticsearch', 'index_name')
    es_type_name = cf.get('elasticsearch', 'doc_type_name')

    # key 配置文件读取
    key_cf = ConfigParser(allow_no_value=True)
    key_cf.read('key_conf')
    sections = key_cf.sections()

    key_data = {}

    for section in sections:
        hosts = [ip for ip, _ in key_cf.items(section)]
        key_data[section.lower()] = hosts

    # command 配置文件读取
    command_cf = ConfigParser()
    command_cf.read('command_conf')
    sections = command_cf.sections()

    command_conf_data = {}

    # command_conf_data = {key --> [username, password, path, [exclude]]}
    for section in sections:
        username = command_cf.get(section, 'username')
        password = command_cf.get(section, 'password')
        path = command_cf.get(section, 'path')
        exclude = json.loads(command_cf.get(section, 'exclude'))
        hosts = [username, password, path, exclude]
        command_conf_data[section.lower()] = hosts

    # 原始数据容器
    raw_queue = Queue(maxsize=1000)
    # ES 插入数据容器
    es_queue = Queue(maxsize=1000)
    # command 线程列队
    command_queue = Queue(maxsize=1000)

    # 实例化数据采集器
    get_data = LogstashMonitor(hosts=es_hosts, index=collection_index_name,
                               queue=raw_queue, count_time=collection_count_time,
                               intervals=collection_intervals)
    get_data_thread = MyThread(get_data.run)
    get_data_thread.start()

    # 数据处理线程
    processor = Processor(raw_data_queue=raw_queue, processing_data_queue=es_queue,
                          compare_data=key_data, command_queue=command_queue)
    processor_thread = MyThread(processor.run)
    processor_thread.start()

    # 数据插入
    es_insert = EsInsert(queue=es_queue, es_index_name=es_index_name,
                         doc_type=es_type_name, es_ip_port=es_hosts)
    es_insert_thread = MyThread(es_insert.run)
    es_insert_thread.start()

    # 命令执行
    command = Command(queue=command_queue, conf=command_conf_data)
    command_thread = MyThread(command.data_worker)
    command_thread.start()
