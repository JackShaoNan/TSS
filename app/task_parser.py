#!/ms/dist/python/PROJ/core/3.4.4/bin/python

import ms.version
ms.version.addpkg('click', '6.7')
ms.version.addpkg('yaml', '3.12-py34')
import os
import yaml
import click
import shutil
import tempfile
import time
import logging
import sqlite3
from multiprocessing import Process


# const vars
APP_START = 'app_start'
HOST_UP = 'host_up'
HOST_DOWN = 'host_down'
SLEEP = 'sleep'
ALLOCATION_CONFIGURE = 'allocation_configure'
CELL = 'test-v3' 
 # app_start
SCHEDULED_PATH = '_scheduled'
APP_FILE = 'app.yml'
 # host_up and host_down
HOST_CREATE_PATH = '_server.presence'
HOST_SERVER_PATH = '_servers'
PLACEMENT = '_placement'
ALLOCATION = 'allocations'


class Task_manager(object):

    def __init__(self, base_path):
        self.base_path = base_path
    
    def __atom_write(self, path, content, file_name, default_flow=False):
        '''
        private function: atom write

        args:
        path : the path where the file will be written 
        content : the content to be written
        file_name : name of the file 
        default_flow : True for str format, False for yaml format

        '''
        #make tempfile
        dirname = os.path.join(self.base_path, path)
        mode = 'wb'
        with tempfile.NamedTemporaryFile(dir=self.base_path,
                                         delete=False, 
                                         mode=mode) as tmpfile:
            yaml.dump(content, encoding=('utf-8'), stream=tmpfile, default_flow_style=default_flow)
        os.rename(tmpfile.name, os.path.join(dirname, file_name))

    def clean_up(self):
        '''
        clean up past files
        '''
        print('in cleanup')
        #clean up _scheduled (apps)
        shutil.rmtree(os.path.join(self.base_path, SCHEDULED_PATH))
        os.mkdir(os.path.join(self.base_path, SCHEDULED_PATH))
        #clean up _server.presence (host)
        shutil.rmtree(os.path.join(self.base_path, HOST_CREATE_PATH))
        os.mkdir(os.path.join(self.base_path, HOST_CREATE_PATH))
        #clean up _placement 
        shutil.rmtree(os.path.join(self.base_path, PLACEMENT))
        os.mkdir(os.path.join(self.base_path, PLACEMENT))
        #clean up allocations
        allocs = []
        self.__atom_write('', allocs, ALLOCATION) 


    def app_start(self, task):
        '''
        produce apps for each type according to the app.yml
        args:
        task : include info about which type and the amount                           
        '''
        
        app_info = {}
        with open(APP_FILE) as app_file:
            app_info = yaml.load(app_file)
        
        app_to_be_produced = {}
        for app in task['apps']:
            app_to_be_produced[app['app']['$ref'].split('#/')[1]] = app['count']
        for a in app_to_be_produced.keys():
            for num in range(app_to_be_produced[a]):
                app_content = {}
                app_content = app_info[a]['manifest']
                file_name = a + "#{0:0>10d}".format(num+1)
                self.__atom_write(SCHEDULED_PATH, app_content, file_name) 
            
                
    def __delete_host(self, host, count):
        '''
        private function : delete host
        args:
        host : host to be deleted
        count : a list includes the index of the hosts' instance
        ''' 
        for index in count:
            host_name = host.split(':')[0][:-5] + "-{0:0>4d}".format(index)
            try:
                os.remove(os.path.join(self.base_path, HOST_CREATE_PATH, host_name))
            except OSError as e:  ## if failed, report it back to the user ##
                print ("Error: %s - %s." % (e.filename, e.strerror))
 
    def host_down(self, task):
        '''
        delete host
        args:
        task : host name to be deleted
        '''
        for host in task['hosts']:
            c_start = int(host.split(':')[0].split('-')[1])
            c_end = c_start + 1
            count = []
            if len(host.split(':')) > 1:
                c_end = int(host.split(':')[1]) + 1
                count = range(c_start, c_end)
            else:
                count = range(c_start, c_end)
                
            self.__delete_host(host, count)
    
    def __create_host(self, host, count):   
        '''
        private function : create host
        args:
        host : host to be created
        count : a list includes the index of the hosts' instance
        '''     
        for index in count:
            host_name = host.split(':')[0][:-5] + "-{0:0>4d}".format(index)
            #get host information
            host_info = {'valid_until': ''}
            with open(os.path.join(self.base_path, HOST_SERVER_PATH, host_name), 'r') as host_file:
                host_obj = yaml.load(host_file)
                host_info['valid_until'] = host_obj['up_since'] + 604800.0
            self.__atom_write(HOST_CREATE_PATH, host_info, host_name, True) 

    def host_up(self, task):
        '''
        create host 
        args:
        task : host name to be created
        '''
        for host in task['hosts']:
            c_start = int(host.split(':')[0].split('-')[1])
            c_end = c_start + 1
            count = []
            if len(host.split(':')) > 1:
                c_end = int(host.split(':')[1]) + 1
                count = range(c_start, c_end)
            else:
                count = range(c_start, c_end)
            
            self.__create_host(host, count)
        
    
    def sleep(self, task):
        '''
        sleep
        args:
        task : give the info about time to sleep
        '''
        time.sleep(task['interval'])

    def allocation_configure(self, task):
        '''
        do allocation configuration
        args:
        task : give the info about configuration
        '''
        #get the information about allocation
        allocs_read = {}
        with open(task['allocation']['$ref'].split('#/')[0]) as allocs:
            allocs_read = yaml.load(allocs)
        alloc_write = {}
        for alloc in allocs_read.keys():
            if alloc == task['allocation']['$ref'].split('#/')[1]:
                alloc_write['_id'] = alloc + '/' + CELL
                alloc_write['name'] = alloc
                alloc_write['cell'] = CELL
                alloc_write['rank'] = 100
                alloc_write['rank_adjustment'] = 10
                alloc_write['partition'] = '_default'
                alloc_write['traits'] = []
                for item in allocs_read[alloc].keys():
                    alloc_write[item] = allocs_read[alloc][item]
        if 'delta' in task:
            for delta in task['delta'].keys():
                alloc_write[delta] = task['delta'][delta]
        #update allocation
        allocations = {}
        with open(os.path.join(self.base_path, 'allocations')) as allocs_list:
            allocations = yaml.load(allocs_list)
        id = alloc_write['_id']
        exist = False
        for alloc in allocations:
            if alloc['_id'] == id:
                exist = True
                for item in alloc_write.keys():
                    alloc[item] = alloc_write[item]
        if not exist:
            allocations.append(alloc_write)
        #save the change to file
        self.__atom_write('', allocations, 'allocations') 

    def run_tasks(self, tasks):
        for task in tasks:
            if task['action'] == APP_START:
                logging.info('task action: app_start')
                self.app_start(task)
                logging.info('app_start complete')
            elif task['action'] == HOST_DOWN:
                logging.info('task action: host_down')
                self.host_down(task)
                logging.info('host_down complete')
            elif task['action'] == HOST_UP:
                logging.info('task action: host_up')
                self.host_up(task)
                logging.info('host_up complete')
            elif task['action'] == SLEEP:
                logging.info('task action: sleep')
                self.sleep(task)
                logging.info('sleep complete')
            elif task['action'] == ALLOCATION_CONFIGURE:
                logging.info('task action: allocation_configure')
                self.allocation_configure(task)
                logging.info('allocation_configure complete')
            else:
                logging.error('wrong action: %s' %task['action'])
                #print('Wrong action: %s' %task['action'])

            
@click.command()
@click.option('--base_path', default='/tmp/snapshot2',
              help='the base path for needed files\' location')
@click.option('--task_file', required=True, 
              help='the task file to be parsed, please spercify the path and file name')
@click.option('--clean_up', default=False,
              help=("whether to clean up the file before running the task," 
              "True for clean, False(by default) for not clean"))
def task_parser(task_file, base_path, clean_up):
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    logging.info('start parsing task')
    with open(task_file) as task_f:
        tasks = yaml.load(task_f)
    task_manager = Task_manager(base_path)
    if clean_up:
        task_manager.clean_up()
    task_manager.run_tasks(tasks) 
    
if __name__ == '__main__':
    task_parser()   
    
