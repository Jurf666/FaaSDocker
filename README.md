运行实验指令：
    tmux new -s server
    . venv/bin/activate
    sudo PYTHON=$(which python3) bash ./run_all_experiments.sh 10 23:00

中断实验指令：
    按 Ctrl+C 即可