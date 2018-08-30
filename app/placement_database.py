#!/ms/dist/python/PROJ/core/3.4.4/bin/python
import ms.version
ms.version.addpkg('click', '6.7')
ms.version.addpkg('yaml', '3.12-py34')
import sqlite3
import logging
import os
import time
import click
import yaml

PLACEMENT = '_placement'
SCHEDULED = '_scheduled'
APP_START = 'app_start'
'''******************************************************************************'''
''' function for getting data and manipulating database'''

        
def scan_and_insert(base_path, t, db, batch):
    #scan scheduled to get app_dict and app_to_be_produced
    app_dict = {}
    app_to_be_produced = {}
    app_dict, app_to_be_produced = get_app_info(base_path)
    # scan directory
    files = os.listdir(os.path.join(base_path, PLACEMENT))
    for file in files:
        if os.path.isdir(os.path.join(base_path, PLACEMENT, file)):
            host = os.listdir(os.path.join(base_path, PLACEMENT, file))
            for app in host:
                app_dict[app.split('#')[0]] += 1 
    #create db table first
    for a in app_to_be_produced.keys():
        create_table(a, db)
    # insert table
    conn = sqlite3.connect(db)
    for app in app_dict.keys():
        c = conn.cursor()
        # get necessary data
        total = app_to_be_produced[app]
        running = app_dict[app]
        pending = total - running
        completed = 0
        # insert
        c.execute('''INSERT INTO %s (BATCH, TIME, TOTAL, RUNNING, PENDING, COMPLETED)
                  VALUES (?, ?, ?, ?, ?, ?)'''%app.replace('.', '__'), (batch, t, total, running, pending, completed))
        logging.info('insert successfully')
        # commit 
        conn.commit()
    # close db
    conn.close() 

def get_app_info(base_path):
    '''scan _scheduled to get app info'''
    app_dict = {}
    app_to_be_produced = {}
    files = os.listdir(os.path.join(base_path, SCHEDULED))
    for app in files:
        app_dict[app.split('#')[0]] = 0
        if app.split('#')[0] in app_to_be_produced:
            app_to_be_produced[app.split('#')[0]] += 1
        else:
            app_to_be_produced[app.split('#')[0]] = 1
    return app_dict, app_to_be_produced
    
    
def create_table(a, db):
    conn = sqlite3.connect(db)
    logging.info('open database successfully')
    # create table
    c = conn.cursor()
    # use rowid, there is no need to define id by myself 
    a = a.replace('.', '__')
    c.execute('''CREATE TABLE IF NOT EXISTS %s
              (BATCH TEXT,
               TIME INT ,
               TOTAL INT,
               RUNNING INT,
               PENDING INT,
               COMPLETED INT);''' %a)
    logging.info('table cteated successfully')
    # commit 
    conn.commit()
    # close db
    conn.close() 
    
    '''******************************************************************************'''

@click.command()
@click.option('--base_path', default='/tmp/snapshot2',
              help='the base path for needed files\' location')
@click.option('--timespan', default=60, 
              help='the whole time period for scan dirs and manipulate db') 
@click.option('--db', default='apps.db', 
              help='spercify the db we want to use')
@click.option('--batch', required=True, 
              help='a string that include infomation about every app-creating')
@click.option('--period', default=5, 
              help='we will scan the dir every %d seconds, %period ')
def get_data(base_path, timespan, db, batch, period):
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    
    # time period for scan and insert
    t = 0
    while timespan >= t:
        logging.info('scan %d' %t)
        scan_and_insert(base_path, t, db, batch)
        time.sleep(period)
        t += period
    logging.info('conpleted')

if __name__ == '__main__':
    get_data()
