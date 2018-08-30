"""Treadmill master scheduler."""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import click
import os
import shutil
import yaml
import tempfile

from treadmill import context
from treadmill import scheduler
from treadmill.scheduler import master
from treadmill.scheduler import zkbackend
from treadmill.scheduler import fsbackend

base_path = '/tmp/snapshot2'
HOST_CREATE_PATH = '_server.presence'
HOST_SERVER_PATH = '_servers'
PLACEMENT = '_placement'
ALLOCATION = 'allocations'
SCHEDULED_PATH = '_scheduled'

def __atom_write(path, content, file_name, default_flow=False):
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
    with tempfile.NamedTemporaryFile(dir=dirname,
                                     delete=False, 
                                     mode=mode) as tmpfile:
        yaml.dump(content, encoding=('utf-8'), stream=tmpfile, default_flow_style=default_flow)
    os.rename(tmpfile.name, os.path.join(dirname, file_name))

def clean_up():
    '''
    clean up past files
    '''
    #clean up _scheduled (apps)
    shutil.rmtree(os.path.join(base_path, SCHEDULED_PATH))
    os.mkdir(os.path.join(base_path, SCHEDULED_PATH))
    #clean up _server.presence (host)
    shutil.rmtree(os.path.join(base_path, HOST_CREATE_PATH))
    os.mkdir(os.path.join(base_path, HOST_CREATE_PATH))
    #clean up _placement 
    shutil.rmtree(os.path.join(base_path, PLACEMENT))
    os.mkdir(os.path.join(base_path, PLACEMENT))
    #clean up allocations
    allocs = []
    __atom_write('', allocs, ALLOCATION) 

def init():
    """Return top level command handler."""

    @click.command()
    @click.option('--once', is_flag=True, default=False,
                  help='Run once.')
    @click.option('--events-dir', type=click.Path(exists=True))
    @click.option('--backendtype', default='zk', type=click.Choice(['fs', 'zk']),
                  help='spercify what kind of backend you want to implement')
    @click.option('--fspath', default='/tmp/snapshot', 
                  help='spercify the path when choose the fsbackend')
    def run(once, events_dir, backendtype, fspath):
        '''clean up at first'''
        #clean_up()
        """Run Treadmill master scheduler."""
        scheduler.DIMENSION_COUNT = 3

        if backendtype == 'fs':
            has_lock = False
            backend = fsbackend.FsBackend(fspath)
        else:
            has_lock = True
            backend = zkbackend.ZkBackend(context.GLOBAL.zk.conn)

        cell_master = master.Master(
            backend,
            context.GLOBAL.cell,
            events_dir
        )
        cell_master.run(once, has_lock)

    return run
