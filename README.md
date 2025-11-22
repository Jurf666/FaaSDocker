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
①sudo docker build -t workflow-proxy:latest .
②在终端1中：sudo venv/bin/python3 controller.py  perf需要sudo权限
③
    a.在终端2中：python3 trigger_workflow.py <workflow_name>
    b.在终端2中：python3 trigger_simple.py <action_name>
        特例1：network需要先在终端3中 python3 /home/jywang/FaaSDocker/actions/network/server.py 
        特例2：couchdb_test需要先启动一个临时的couchDB 
        sudo docker run -d \
            --name couchdb-test \
            -p 5984:5984 \
            -e COUCHDB_USER=openwhisk \
            -e COUCHDB_PASSWORD=openwhisk \
            apache/couchdb:2.3

目前已实现：限制容器只获得0.2个时间片时，各种action的精简运行指标采集

后续目标：①有点忘了简单action是否被自己魔改过，需要检查一下②按照现在的逻辑把所有action重新跑一遍③在FaaSFlow中，工作流的数据传递是利用数据库的，但是被自己魔改成了在主机上开辟一片共享目录，是否有影响？④保证实验数据没有问题后，确定到底用什么方法来进行分组
        

