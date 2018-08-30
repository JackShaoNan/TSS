#!/bin/bash
# Basic until loop

for i in {1..1..1}
do
    sleep 2
    ./clean_up.py
    sleep 3
    ./task_parser.py --task_file task.yml
    sleep 5
    /v/global/user/s/sh/shaonan/stash/cloud.treadmill-core/cloud/treadmill-core/treadmill-core/src/venv/bin/treadmill sproc --cell test-v3 --logging-conf daemon_container.json scheduler --backendtype fs --fspath /tmp/snapshot2 &
    sleep 10
    ./task_parser.py --task_file task1.yml &
    ./placement_database.py --batch $i --period 2 &
    sleep 80
    id=$(ps -a | grep treadmill | awk '{print $1}')
    kill -9 $id
done
