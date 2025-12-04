操作步骤：
①sudo docker build -t jywang_test .
②在终端1中：sudo venv/bin/python3 controller.py  （perf需要sudo权限）
③做好workflow与特殊action的前置工作
1)sudo docker run -d --name redis -p 6379:6379 redis
2)sudo docker run -d \
  --name couchdb \
  -p 5984:5984 \
  -e COUCHDB_USER=openwhisk \
  -e COUCHDB_PASSWORD=openwhisk \
  apache/couchdb
3)sudo docker run -d \
            --name couchdb-test \
            -p 5984:5984 \
            -e COUCHDB_USER=openwhisk \
            -e COUCHDB_PASSWORD=openwhisk \
            apache/couchdb:2.3
4)在终端3中 python3 /home/jywang/FaaSDocker/actions/network/server.py 
④python3 run_experiment.py (只负责触发函数执行)
⑤python3 aggregate_metrics.py （读取所有json监测结果，取平均，并计算分组所需要的派生子指标）
⑥python3 grouping.py（进行实际分组）

