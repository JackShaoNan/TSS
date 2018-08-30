#!/ms/dist/python/PROJ/core/3.4.4/bin/python
import ms.version
ms.version.addpkg('click', '6.7')
ms.version.addpkg('yaml', '3.12-py34')
ms.version.addpkg('matplotlib', '2.0.2-py34')
ms.version.addpkg('six', '1.11.0-ms1')
ms.version.addpkg('numpy', '1.14.2-py34')
ms.version.addpkg('pyparsing', '2.2.0')
ms.version.addpkg('cycler', '0.10.0')
ms.version.addpkg('dateutil', '2.7.2')


import pyparsing
import os
import time
from multiprocessing import Process
import sqlite3
import logging
import matplotlib.pyplot as plt
import numpy as np
import six
import click

@click.command()
@click.option('--db', default='apps.db', 
              help='spercify the db we want to use')
@click.option('--des_path', default='.', 
              help='spercify the path that picture will be saved')
@click.option('--mode', default='unit', type=click.Choice(['divide', 'unit']), 
              help='divide for saving pics seperately, unit for saving in the same figure')           
@click.option('--batch', required=True, 
              help='spercify which part of data you want to use to draw the figure,' 
              'if input is __ALL__, it is running multi batch test')
def run(db, des_path, mode, batch):
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    conn = sqlite3.connect(db)
    logging.info('open database successfully')
    c = conn.cursor()
    res = c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    res = res.fetchall()
    if batch == "__ALL__":
        index = 0
        # for different proid
        for name in res:
            index += 1
            if mode == 'unit':
                c1 = conn.cursor()
                batchs = c1.execute("select batch from %s where time=60 order by rowid" %name[0])
                for b in batchs:
                    x = []
                    y = []
                    c2 = conn.cursor()
                    points = c2.execute("SELECT TIME, PENDING FROM %s WHERE BATCH=?"%name[0], (b))
                    for point in points:
                        x.append(point[0])
                        y.append(point[1]) 
                    plt.subplot(len(res), 1, index)
                    plt.xlim((0, x[-1]))
                    #plt.xlabel('sample points')
                    #plt.ylabel('pending apps')
                    #plt.scatter(x, y, s=0.1)
                    plt.plot(x, y, linewidth=0.1)
            else:
                # divide mode
                plt.figure()
                c1 = conn.cursor()
                batchs = c1.execute("select batch from %s where time=60 order by rowid" %name[0])
                for b in batchs:
                    x = []
                    y = []
                    c2 = conn.cursor()
                    points = c2.execute("SELECT TIME, PENDING FROM %s WHERE BATCH=?"%name[0], (b))
                    for point in points:
                        x.append(point[0])
                        y.append(point[1])
                    plt.xlim((0, x[-1]))
                    #plt.xlabel('sample points')
                    #plt.ylabel('pending apps')
                    plt.plot(x, y, linewidth=0.1)
                plt.savefig(os.path.join(des_path, name[0] + '_multi_batch.png'))
    else:
        index = 0
        for name in res:
            index += 1
            x = []
            y = []
            c1 = conn.cursor()
            # we must put a comma in the end of (), cuz we must make the paras a tuple
            points = c1.execute("SELECT TIME, running FROM %s WHERE BATCH=?" %name[0], (batch,))
            for point in points:
                x.append(point[0])
                y.append(point[1])
            if mode == 'unit':
                plt.subplot(len(res), 1, index)
                plt.xlim((0, x[-1]))
                plt.ylim((0, 4000))
                plt.xlabel('unit: second')
                #plt.ylabel('pending apps')
                plt.title('Running Instances of application')
                plt.annotate(r'$500\ hosts\ shut\ down\ here$', xy=(23, 3750), xytext=(30, 3000), fontsize=13, arrowprops=dict(facecolor='black', shrink=0.05))
                plt.plot(x, y, linewidth=10)
            else:
                plt.figure()
                plt.plot(x, y, linewidth=10)
                plt.xlim((0, x[-1]))
                #plt.xlabel('sample points')
                #plt.ylabel('pending apps')
                plt.savefig(os.path.join(des_path, name[0] + '.png'))
    if mode == 'unit':
        if batch != '__ALL__':
            #plt.annotate(r'$this\ is\ where\ 10\ hosts\ created$', xy=(51, 58), xytext=(0, 50), fontsize=18, arrowprops=dict(facecolor='black', shrink=5))
            plt.savefig(os.path.join(des_path, db + '.png'))
        else:
            #plt.annotate(r'$this\ is\ where\ some\ hosts\ shut\ down$', xy=(46, 3900), xytext=(10, 3250), arrowprops=dict(facecolor='black', shrink=0.05))
            plt.savefig(os.path.join(des_path, db + '_multi_batch.png'))
    conn.commit()
    conn.close()

if __name__ == '__main__':
    run()

