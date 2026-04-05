操作步骤： 
①sudo docker build -t jywang_test . 
②清理所有容器 sudo docker rm -fv $(sudo docker ps -aq)
③清理 Linux 文件系统缓存
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches
④做好workflow与特殊action的前置工作 
1)sudo docker run -d --name redis --cpuset-cpus="1" --cpuset-mems="1" -p 6379:6379 redis 
2)sudo docker run -d --name couchdb-test --cpuset-cpus="3" --cpuset-mems="1" -p 5984:5984 -e COUCHDB_USER=openwhisk -e COUCHDB_PASSWORD=openwhisk apache/couchdb:2.3 
3)在终端3中 python3 ./actions/network/server.py 
⑤在终端1中：
    python3 controller.py > base.log 2>&1 
    python3 controller.py > exp.log 2>&1

0.2period4client：
    实验组：使用容器创建时默认绑定的cpu列表（即分配给函数的一个物理核）
    baseline：请求到来时随机更新容器所绑定的物理核（是否有必要设置一个上限）

1period2client：由于实验组的特性，要求不可以同时有多个任务重叠在同一个逻辑核上
    实验组：容器创建时各自绑定一个逻辑核
    baseline：请求到来时随机更新容器所绑定的逻辑核；每一个逻辑核设置锁，仅可从未被上锁的逻辑核中随机挑选

使用htop指令观察当前时刻机器是否在运行任务