#!/ms/dist/python/PROJ/core/3.4.4/bin/python

import ms.version
ms.version.addpkg('click', '6.7')
ms.version.addpkg('yaml', '3.12-py34')
import os
import yaml
import click
import tempfile

# const var
SERVERS_PATH = '_servers'
SERVER_PRESENCE_PATH = '_server.presence'

def __create(host_list, to_path, from_path):
    for host in host_list:
        if not os.path.isfile(os.path.join(to_path, host)):
            #get host information
            host_info = {'valid_until': ''}
            with open(os.path.join(from_path, host), 'r') as host_file:
                host_obj = yaml.load(host_file)
                host_info['valid_until'] = host_obj['valid_until']
            #make tempfile
            dirname = to_path
            mode = 'wb'
            with tempfile.NamedTemporaryFile(dir=dirname,
                                             delete=False, 
                                             mode=mode) as tmpfile:
                yaml.dump(host_info, encoding=('utf-8'), stream=tmpfile)
            os.rename(tmpfile.name, os.path.join(to_path, host))
 
def __delete(host_list, to_path):
    for host in host_list:
        try:
            os.remove(os.path.join(to_path, host))
        except OSError as e: 
            if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
                raise # re-raise exception if a different error occurred
            
@click.command()
@click.option('--hosts_file', required=True, help='the host file to be manipulated which may include several hosts, full path and name should be given')
@click.option('--opration', required=True, type=click.Choice(['create', 'delete']), help='spercify the opration to manipulate the file')
@click.option('--from_path', default='/tmp/snapshot2', help='the path where the servers is (a diractory that _servers located in)')
@click.option('--to_path', default='/tmp/snapshot2', help='the path where the server.presence is (a diractory that _server.presence located in)')
def host_opration(hosts_file, opration, from_path, to_path):
    from_path = os.path.join(from_path, SERVERS_PATH)
    to_path = os.path.join(to_path, SERVER_PRESENCE_PATH)

    host_list = []
    with open(hosts_file, 'r') as hosts:
        host_list = yaml.load(hosts)
    if opration == 'create':
        __create(host_list, to_path, from_path)
    else:
        __delete(host_list, to_path)

if __name__ == '__main__':
    host_opration()
