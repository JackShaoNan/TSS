#!/ms/dist/python/PROJ/core/3.4.4/bin/python
import ms.version
ms.version.addpkg('click', '6.7')
ms.version.addpkg('yaml', '3.12-py34')
import os
import yaml
import click
import time
import logging

BUCKETS = '_buckets'
CELL = '_cell'
SERVERS = '_servers'
# global var for counting the number of hosts for each type
host_num = {}

'''produce the yaml file for host
 Args:
    host_info: the dict that include host info
    type: host type
    rack: index of rack
    number: host number
    path: the path where the host file will be written
'''
def produce(host_info, host_type, rack, number, base_path):
    for i in range(number):
        host_num[host_type] += 1
        name = "{0:0>4d}".format(host_num[host_type])
        with open(os.path.join(base_path, SERVERS, host_type) + "-" + name, 'w') as f_new:
            host = {'parent': rack}
            for args in host_info[host_type].keys():
                host[args] = host_info[host_type][args]
            yaml.dump(host, f_new, default_flow_style=False)


@click.command()
@click.option('--f_host', default='hosts.yml', help='host file that includes hosts information')
@click.option('--f_topo', default='topology.yml', help='topology file that includes info of each rack')
#@click.option('--cell', required=True, help='spercify the cell, we will put it to "parent name"')
@click.option('--base_path', default='/tmp/snapshot2', help='the path where the host file will be written')
def run_host_producer(f_host, f_topo, base_path):           
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    #scan the hosts.yml and get the type of hosts
    with open(f_host) as host_file:
        host_dict = yaml.load(host_file)
    host_info = host_dict

    for h in host_dict.keys():
        host_num[h] = 0
        
    # from topology.yml
    with open(f_topo) as topo_file:
        topo = yaml.load(topo_file)

    for building in topo.keys():
        #save building in _buckets and _cell
        logging.info('saving parent buckets into _cell/')
        with open(os.path.join(base_path, CELL, building), 'w') as cell:
            pass
        logging.info('saving all buckets into _buckets/')
        with open(os.path.join(base_path, BUCKETS, building), 'w') as buildings:
            content = {'parent': None, 'traits': 0}
            yaml.dump(content, buildings)
        #get info for current building
        for rack in topo[building].keys():
            logging.info('producing hosts in %s' %rack)
            #save rack in _buckets
            with open(os.path.join(base_path, BUCKETS, rack), 'w') as racks:
                content = {'parent': building, 'traits': 0}
                yaml.dump(content, racks)
            #host_info[rack] = rack
            for element in topo[building][rack]:
                number = element['number']
                host_type = element['type']['$ref'].split('#/')[1]
                host_info[host_type]['up_since'] = time.time()
                host_info[host_type]['partition'] = '_default'
                produce(host_info, host_type, rack, number, base_path)

if __name__ == '__main__':
    run_host_producer()
