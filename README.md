分组文件：baseline_groups.json task_groups.json
核心代码：controller.py function_manager.py proxy.py store.py 
测试脚本：run_experiment_closed_loop.py
(其余代码文件与当前实验无关)

操作步骤： 
①sudo docker build -t jywang_test . 
②在终端1中：
    python3 controller.py > base.log 2>&1 
    python3 controller.py > exp.log 2>&1
③做好workflow与特殊action的前置工作 
1)sudo docker run -d --name redis --cpuset-cpus="1" --cpuset-mems="1" -p 6379:6379 redis 
2)sudo docker run -d --name couchdb-test --cpuset-cpus="3" --cpuset-mems="1" -p 5984:5984 -e COUCHDB_USER=openwhisk -e COUCHDB_PASSWORD=openwhisk apache/couchdb:2.3 
3)在终端3中 python3 ./actions/network/server.py 

sudo docker rm -f $(sudo docker ps -aq)