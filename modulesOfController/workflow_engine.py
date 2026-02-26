import threading
import logging
import modulesOfController.definitions as definitions

logger = logging.getLogger("WorkflowEngine")

class WorkflowEngine:
    def __init__(self, dispatcher, data_store):
        self.dispatcher = dispatcher
        self.data_store = data_store
        
    def submit_task(self, name, payload, task_id):
        t = threading.Thread(
            target=self._run_async, 
            args=(name, payload, task_id)
        )
        t.start()
        
    def _run_async(self, name, payload, task_id):
        subtasks = []
        try:
            self.data_store.update_task_status(task_id, "running")
            
            # 路由逻辑
            if name == "video":
                subtasks = definitions.workflow_video(self.dispatcher, self.data_store, payload)
            elif name == "recognizer":
                subtasks = definitions.workflow_recognizer(self.dispatcher, self.data_store, payload)
            elif name == "svd":
                subtasks = definitions.workflow_svd(self.dispatcher, self.data_store)
            elif name == "wordcount":
                subtasks = definitions.workflow_wordcount(self.dispatcher, self.data_store)
            else:
                raise Exception(f"Unknown workflow: {name}")
                
            self.data_store.update_task_status(task_id, "completed", subtasks=subtasks)
            logger.info(f"Task {task_id} ({name}) completed.")
        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")
            self.data_store.update_task_status(task_id, "failed", subtasks=subtasks)