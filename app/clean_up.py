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
import sqlite3
import logging

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

def __atom_write(base_path, path, content, file_name, default_flow=False):
        '''
        private function: atom write

        args:
        path : the path where the file will be written 
        content : the content to be written
        file_name : name of the file 
        default_flow : True for str format, False for yaml format

        '''
        #make tempfile
        dirname = os.path.join(base_path, path)
        mode = 'wb'
        with tempfile.NamedTemporaryFile(dir=base_path, 
                                         delete=False, 
                                         mode=mode) as tmpfile:
            yaml.dump(content, encoding=('utf-8'), stream=tmpfile, default_flow_style=default_flow)
        os.rename(tmpfile.name, os.path.join(dirname, file_name))

def clean(base_path, path):
        logging.info("clean %s", path)
        files = os.listdir(os.path.join(base_path, path))
        for file in files:
            if os.path.isdir(os.path.join(base_path, path, file)):
                shutil.rmtree(os.path.join(base_path, path, file))
            else:
                os.remove(os.path.join(base_path, path, file))

@click.command()
@click.option('--base_path', default='/tmp/snapshot2', 
              help='spercify the base path we want to use')
@click.option('--db', default='apps.db', 
              help='spercify the db we want to use')
@click.option('--clean_db', default=False, 
              help='True for cleanning db, False for not clean')
def clean_up(base_path, db, clean_db):
    '''
    private function: clean up past files
    '''
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    clean(base_path, SCHEDULED_PATH)
    clean(base_path, HOST_CREATE_PATH)
    clean(base_path, PLACEMENT)
    #clean up allocations
    logging.info("clean allocations")
    allocs = []
    __atom_write(base_path, '', allocs, ALLOCATION)
    if clean_db:
        logging.info("clean db")
        try:
            os.remove(db)
        except Exception as e:
            logging.info("remove %s error!" %db)
    logging.info("clean completed")

if __name__ == '__main__':
    clean_up()
