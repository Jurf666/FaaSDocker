运行实验指令：
    tmux new -s client
    source .venv/bin/activate
    bash ./run_all_experiments.sh 10

中断实验指令：
    sudo pkill -f "run_all_experiments.sh"
    sudo pkill -f "run.py"
