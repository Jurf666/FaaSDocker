TASK_GROUPS_FILE=baseline_groups.json python3 run.py > base.log 2>&1
TASK_GROUPS_FILE=task_groups.json python3 run.py > exp.log 2>&1

python3 jsonAnalyze.py 
python3 jsonAnalyzeFiltered.py 

python3 visualize.py