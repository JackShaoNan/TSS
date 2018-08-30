# Treadmill Simulator
Treadmill Scheduler Simulator is my summer intern project, which is aimed to implement tools to help treadmill developer to do test or
simulation work on treadmill easily. My work mainly includes three parts: migrating treadmill to file-system backend, implementing tools 
to set specific test scenario and tools to visualize the behavior of treadmill scheduler.

Here is the MS documentation of Treadmill:  http://treadmill/

---
This README file contains instructions about how to use the treadmill simulator.


Migrating Treadmill to fs-backend
---
Treadmill Scheduler is based on ZooKeeper for distributed storage. But for test work, we would like to get rid of all those dependencies 
and build up treadmill quickly. So we migrate it to file-system backend, which is very similar to ZooKeeper. 

To support multi-choice(ZooKeeper or file-system) when start the treadmill, please use __treadmill-simulator/fsbackend/master.py__ and 
__treadmill-simulator/fsbackend/scheduler.py__ to replace the __/treadmill-core/src/lib/python/treadmill/scheduler/master.py__ and 
__/treadmill-core/src/lib/python/treadmill//sproc/scheduler.py__ respectively.
To start treadmill, run:

  `/venv/bin/treadmill sproc --cell cellname scheduler`
  
  (You should spercify the backend type and for more options, use `--help` to explore more of this command)
  
  
Setting spercific test scenarios
---
Setting a specific test scenario is actually parsing a __'task.yml'__ file and then executing the actions in it. Actions includes adding 
or removing different kind of host servers, starting different kind of applications(tasks), configuring allocation and sleep. Different
combinations of actions constitute different test scenarios. To successfully set up a test scenario, please follow steps below(also please 
refer to the __'Treadmill Scheduler Simulator - Outline and Scope.docx'__):

1. Set hosts(see code in host/ and config/)
    1. Edit the __hosts.yml__ (in config/) to specify configurations of different kind of servers
    2. Edit the __topology.yml__ (in config) to specify the topology relations in treadmill cell(such as rack name, server quantities and the like)
    3. Run __host_producer.py__ (in host/) to create virtual hosts(will be appear in __/snapshot/_servers/__). run:
         `./host_producer.py`
         (You should specify the configure files and for more options, use `--help` to explore more of this commands)
2. Set apps and whole test scenario(see code in config/)
    1. Edit the __app.yml__ to specify configurations of different kind of applications
    2. Edit the __allocation.yml__ to specify configurations of different kind of allocations
    3. Edit the __task.yml__ to specify the different combinations of actions for your test scenario

Parse task and put the result into sqlite3 database (see code in app/)
---
The result here represents the number of whole apps in treadmill, running apps and pending apps(Obviously, running plus pending 
equals to total). To run the test case specified in task yaml file, we should do:

1. (Optional) Clean history data in treadmill cell first, run:
    
    `./clean_up.py`
    
    In this way, we clean all data in **_server.presence/, _scheduled/, _placement/, allocations**
    
    Again, using `--help` to explore more of this commands.
    
2. To start treadmill with fs-backend, run:

    `/venv/bin/treadmill sproc --cell cellname scheduler --backendtype fs --fspath pathname`
    
    
3. To parse task and run it on treadmill, run **task_parser.py**(in a new terminal window):
     
     `./task_parser.py --task_file filename`
     
4. To get data about changes of application states, run **./placement_database.py**:
     
     `./placement_database.py --batch batchname`
     
     In this way, we put result into sqlite3 database. Using `--help` to explore more of this commands.
     
5. Actually, to record the changes of application states, we must run **task_parser.py** and **placement_database.py** at the same time
(Because treadmill shceduler reacts to such test quickly, we should get data in this short time period). So run:
     
     `./task_parser.py --task_file taskfile & ./placement_database.py --batch batchname &`
     
     
Monte Carlo Simulation (see code in app/)
---
For really complicated test scenario, we can not predict what will happen with treadmill scheduler, so we would like to repeat such uncertain situations very large amount of times to get a raletive certain result with the maximum possibility. It is called Monte Carlo 
Simulation. Run:

`./monte_carlo_test.sh`
     
Please edit **monte_carlo_test.sh**, to customise your own test case.
     
PS: we could make use of this script to wrap up 5 steps above and run them in one command, which can be much easier.




Morgan Stanley Core Infrastructure Enterprise Computing Team
Treadmill scheduler simulator
1. Migrated treadmill to file-system backend
2. Implemented tools to set test scenarios and get visualized result
3. Implemented tool to launch Monte Carlo simulation on treadmill 
