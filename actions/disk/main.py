import subprocess
import time
import os # 引入os用于删除文件

tmp = '/proxy/exec/actions/disk/tmp/'

"""
dd - convert and copy a file
man : http://man7.org/linux/man-pages/man1/dd.1.html
Options
 - bs=BYTES
    read and write up to BYTES bytes at a time (default: 512);
    overrides ibs and obs
 - if=FILE
    read from FILE instead of stdin
 - of=FILE
    write to FILE instead of stdout
 - count=N
    copy only N input blocks
"""
def main(param):
    #bs = 'bs='+param.get('bs')
    #count = 'count='+param.get('count')
    bs = 'bs='+str(param["bs"])
    count = 'count='+str(param["count"])
    
    # 修改1: 输出到真实文件，而不是 /dev/null
    output_file = tmp + 'io_test.dat'
    of_param = 'of=' + output_file
   
    out_fd = open(tmp + 'io_write_logs', 'w')
    start = time.time()
    # 修改2: 加入 conv=fdatasync 强制物理落盘，否则只是写内存缓存
    #dd = subprocess.Popen(['dd', 'if=/dev/zero', 'of=/dev/null', bs, count],stderr=out_fd)
    dd = subprocess.Popen(['dd', 'if=/dev/zero', of_param, bs, count, 'conv=fdatasync'], stderr=out_fd)
    dd.communicate()
    subprocess.check_output(['ls', '-alh', tmp])
    latency = time.time()-start
    out_fd.close() # 顺手关一下日志文件句柄
    #with open(tmp + 'io_write_logs') as logs:
        #result = str(logs.readlines()[2]).replace('\n', '')
        #result = logs.readlines()
      # 修改3: 运行完删除那个大文件，防止硬盘爆满
    if os.path.exists(output_file):
        os.remove(output_file)
    print('latency :',latency)
    return {
        "latency": latency,
        "throughput_mb_s": (int(param["count"]) * 1.0) / latency if 'M' in str(param["bs"]) else 0
    }

#main({"bs":2048,"count":50000})

