import docker

def verify_cpu_pinning():
    client = docker.from_env()
    containers = client.containers.list()
    
    print(f"{'CONTAINER ID':<15} {'NAME':<30} {'CPUSET (Docker Config)'}")
    print("-" * 65)
    
    for c in containers:
        # 获取容器的配置信息
        config = c.attrs['HostConfig']
        cpuset = config.get('CpusetCpus', 'Not Set')
        
        # 过滤掉非实验相关的容器（可选）
        # if "yyxie" not in c.name: continue
        
        print(f"{c.id[:12]:<15} {c.name[:30]:<30} {cpuset}")

if __name__ == "__main__":
    verify_cpu_pinning()