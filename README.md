# proxy server in container

## filesystem structure
- `/proxy/exec/`: the dir where extracted files stay
- `/proxy/exec/main.py`: the default main program of action
- `/proxy/ActionRunner.py`: the proxy server

the working directory of proxy server should be `/proxy/exec/`

## API
server runs at port 5000 in the container. it receives the following request:
- `/status`: GET request. return a json. get the status including `new`, `init`, `run`, and `ok`. the action name is sended after init.
- `/init`: POST request. do the initialization like decrypting and extracting.
- `/run`: POST request. return a json. to actually run the action.

### status
the meaning of each status:
- new: a new container before doing init
- init: currently doing the initialization
- run: currently handling a request
- ok: wait for a request

### init
must send a json object in the following form:
```json
{
    "action": "test"
}
```

the meaning of each field:
- action: the action name. action's code should be placed first in directory `/proxy/exec`.

### run
must send a json object. it will be used as the input of the action.

操作步骤：
①sudo docker build -t jywang_test .
②在终端1中：sudo venv/bin/python3 controller.py  （perf需要sudo权限）
③运行实际的任务：
    a.如何运行workflow：
    在终端2中(目前现在用curl指令的形式)：
        1)
sudo docker run -d --name redis -p 6379:6379 redis
        2)
sudo docker run -d \
  --name couchdb \
  -p 5984:5984 \
  -e COUCHDB_USER=openwhisk \
  -e COUCHDB_PASSWORD=openwhisk \
  apache/couchdb
        3)
    curl -X POST http://localhost:5000/dispatch_workflow \
     -H "Content-Type: application/json" \
     -d '{"workflow_name": "wordcount"}'

    b.如何运行简单action：
    在终端2中：python3 trigger_simple.py <action_name>
        特例1：network需要先在终端3中 python3 /home/jywang/FaaSDocker/actions/network/server.py 
        特例2：couchdb_test需要先启动一个临时的couchDB 
        sudo docker run -d \
            --name couchdb-test \
            -p 5984:5984 \
            -e COUCHDB_USER=openwhisk \
            -e COUCHDB_PASSWORD=openwhisk \
            apache/couchdb:2.3

暂时没想到还可能会有什么问题，后续再复盘一下吧


