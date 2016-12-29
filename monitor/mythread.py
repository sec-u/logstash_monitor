#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# @Time    : 16/11/2 14:44


import logging
from threading import Thread

logger = logging.getLogger('MyThread')


class MyThread(Thread):
    def __init__(self, func, args=(), kwargs=None):
        super(MyThread, self).__init__()
        if kwargs is None:
            kwargs = {}

        self._func = func
        self._args = args
        self._kwargs = kwargs

    def run(self):
        """
        __module__: 该函数定义的模块名；
        __name__: 函数定义时的函数名；
        im_class: 实际调用该方法的类，或实际调用该方法的实例的类。仅仅是函数的时候此方法报错
        """
        try:
            im_class = self._func.im_class
        except AttributeError:
            im_class = "function"

        logger.info('Created new thread %s %s %s' % (self._func.__module__, self._func.__name__, im_class))

        try:
            if self._func:
                self._func(*self._args, **self._kwargs)
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._func, self._args, self._kwargs
