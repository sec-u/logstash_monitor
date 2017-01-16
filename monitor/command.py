#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# @Time    : 16/11/2 14:46

import logging
import subprocess
from mythread import MyThread

logger = logging.getLogger('Command')


class Command(object):
    def __init__(self, queue, conf):
        # 原始队列
        self.raw_queue = queue
        self.conf = conf

    @staticmethod
    def command(ip, username, password, path, log):
        """ 执行命令
        command_str = ('/home/monitor/bin/sshpass -p "%s" ssh %s@%s  -o '
                       'StrictHostKeyChecking=no  "sh %s 3"' % (password, username, ip, path))
        """
        hosts = '%s@%s' % (username, ip)
        bash = 'sh %s 3' % path
        command_list = ['/home/monitor/bin/sshpass', '-p', password, 'ssh', hosts,
                        '-o', 'StrictHostKeyChecking=no', bash]
        log.info(' '.join(command_list))

        # 执行命令
        subpro = subprocess.Popen(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # 阻塞等待执行结果
        exit_code = subpro.wait()
        stdout, stderr = subpro.communicate()

        log.info("%s command exit_code is %s" % (ip, exit_code))
        log.info("%s stdout: %s" % (ip, stdout))
        log.info("%s stderr: %s" % (ip, stderr))

    def compare(self, data):
        """ 比对配置文件是否有此数据的 key """
        key = data['name']
        ip = data['ip']

        if key in self.conf:
            logger.info("%s did not respond!" % ip)

            # 判断是否在配置列表里面
            if ip not in self.conf[key]:
                logger.info("%s %s isn't in command config file!" % (key, ip))
                return

            username = self.conf[key][ip][0]
            password = self.conf[key][ip][1]
            path = self.conf[key][ip][2]

            logger.info("Create thread to %s Command" % ip)
            thread = MyThread(self.command, (ip, username, password, path, logger))
            thread.start()

    def data_worker(self):
        """ 数据处理 """
        while True:
            # 获取原始数据
            data = self.raw_queue.get()

            # 原始数据整理、拆分
            for i in data:
                # 检查配置文件是否有 key，如有执行命令
                self.compare(i)
