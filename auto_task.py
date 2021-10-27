# -*- encoding: utf-8 -*-
'''
@File    :   auto_task.py
@Time    :   2021/10/22 15:48:29
@Author  :   hongchunhua
@Contact :   
@License :   (C)Copyright 2020-2025
'''

import re
import time
import sys, getopt
import os
import threading
import logging
import zipfile

class TaskLogger(logging.Logger):
    def __init__(self, name, level='INFO', file=None, encoding='utf-8'):
        super().__init__(name)
        self.setLevel(level=level)
        stdio = logging.StreamHandler()
        stdio.setLevel(level=level)
        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s:%(funcName)s@%(lineno)d] %(message)s")
        stdio.setFormatter(formatter)
        self.addHandler(stdio)

#全局默认日志模块
G_DEFAULT_LOGGER = TaskLogger("Task")

class BaseTask(object):
    """
    ##说明：
        该类用于注册定时任务
    ##使用方法      
    """
    def __init__(self, interval=60, logger=G_DEFAULT_LOGGER):
        self._is_runing = False
        self.interval = interval
        self._sig = threading.Event()
        self.name = 'task'
        self.left_time = 0
        self.logger = logger
    
    def init(self):
        is_ok = True
        self.left_time = self.interval
        return is_ok
        
    def perid_do(self):
        is_exit = True
        self.logger.info("do nothing")
        return is_exit
    
    def run(self):
        try:
            self.logger.info("Task name: "+ self.name +' wake up.')
            self.left_time = self.interval
            return self.perid_do()
        except Exception as e:
            self.logger.error("run task[{0}] err: {1}".format(self.name, e))
            raise e

    def exit(self):
        self.is_runing = False

    def is_expire(self):
        return (self.left_time == 0)
    
    def add_clock(self, spend_time):
        if self.left_time > spend_time:
            self.left_time -= spend_time
        else:
            self.left_time = 0
        return False

    def __enter__(self):
        self.run()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

class AutoTask(threading.Thread):
    def __init__(self, TaskList=[], logger=G_DEFAULT_LOGGER, tick=5):
        super(AutoTask, self).__init__(name="autotask")
        self.is_running = False
        self.is_exit = False
        self.task_list = TaskList
        self._sig = threading.Event()
        self.tick = tick
        self.logger = logger
        
    def run(self):
        self.is_running = True
        self.is_exit = False
        while self.is_running:
            to_remove_task =[]
            for task in self.task_list:
                is_exit = False
                if task.is_expire():
                    if task.run():
                        to_remove_task.append(task)
                task.add_clock(self.tick)
            for task in to_remove_task:
                self.logger.warning("Remove task: " + task.name)  
                self.task_list.remove(task)
            self._sig.wait(self.tick)
        self.is_exit = True
        self._sig.set()

    def stop(self):
        self.is_running = False
        while not self.is_exit:
            self._sig.set()
            time.sleep(1)
    
    def add_task(self, task):
        if isinstance(task, BaseTask):
            self.task_list.append(task)

    def add_tasklist(self, tasklist):
        for task in tasklist:
            if isinstance(task, BaseTask):
                self.task_list.append(task)

    def __enter__(self):
        self.run()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

class ZksConfCleanTask(BaseTask):
    """
    ##说明：
        该类用于清理动态配置文件
    ##使用方法       
    """
    def __init__(self, path, interval=15, max_size=100, max_file_num=5):
        """
        ##参数说明：
            path            配置文件路径
            interval        清理间隔，单位秒
            max_size        限制空间使用大小，单位M
            max_file_num    动态配置文件数量上限值
        """
        # 继承父类初始化参数,必须优先调用
        BaseTask.__init__(self, interval = interval)
        self.interval = interval
        self.name = 'Zks Conf Clean'
        self.path = path
        self.max_num = max_file_num
        self.max_size = max_size
        if not os.path.isdir(path):
            raise ValueError("invalid zookeeper cfg path.")
        
    def perid_do(self):
        try:
            self._backup_cfg()
        except Exception as e:
            self.logger.error("perid_do err: {}".format(e))
            raise e
        return False
    
    def _file2key(self, file=""):
        (_, _, _, str_sn) = file.split('.')
        return int(str_sn, 16)

    def _zip2key(self, file=""):
        ( _, str_sn, _) = file.split('.')
        return int(str_sn, 16)

    def _backup_cfg(self):
        cfg_files=[]
        zip_files=[]
        current_size = 0
        for file in os.listdir(self.path):
            if not os.path.isfile(os.path.join(self.path, file)):
                continue
            if re.match("zoo\.cfg\.dynamic\.(.?)", file):
                cfg_files.append(file)
                continue
            if re.match("dynamic\.(.?)\.zip", file):
                zip_files.append(file)
                current_size += os.path.getsize(os.path.join(self.path, file))
                continue
        # 压缩备份配置文件
        if len(cfg_files) > self.max_num:
            cfg_files.sort(key=self._file2key)
            zip_file = zipfile.ZipFile(os.path.join(self.path, "dynamic."+cfg_files[0]+'.zip'), 'w')
            for i,file in  enumerate(cfg_files):
                if i > (len(cfg_files) - self.max_num):
                    break
                zip_file.write(os.path.join(self.path, file),compress_type=zipfile.ZIP_DEFLATED)
                os.remove(os.path.join(self.path, file))
            zip_file.close()
        # 达到空间上限，删除一半压缩备份
        if (current_size//(1024*1024)) > self.max_size:
            zip_files.sort(key=self._zip2key)
            for i,zfile in enumerate(zip_files):
                if i > len(zip_files):
                    break
                os.remove(os.path.join(self.path, zfile))

def main(argv):
    try:
        opts, args = getopt.getopt(argv[1:], "p:i:", ["path=",'interval='])
        mypath = ''
        my_interval = 10
        if len(opts) == 0 and len(args):
            raise ValueError("input invalid")
        for cmd, val in opts:
            if cmd in ('-p', '--path'):
                mypath = val
                continue
            elif cmd in ('-i', '--interval'):
                my_interval = int(val)
                continue
            else:
                raise ValueError("parameter invalid: %s,%s" %(cmd, val))
        mytask = AutoTask(TaskList=[ZksConfCleanTask(path=mypath, interval=my_interval)])
        mytask.start()
        time.sleep(60)
        mytask.stop()
        mytask.join()

    except Exception as e:
        print("main: {}".format(e))
        raise e

if __name__ == "__main__":
    main(sys.argv)
            
