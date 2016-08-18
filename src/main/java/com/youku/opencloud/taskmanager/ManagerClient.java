/**
 * 
 */
package com.youku.opencloud.taskmanager;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.callback.OnManagerCallback;
import com.youku.opencloud.constant.ZKNodeConst;

/**
 * @author liulietao
 *
 */
public class ManagerClient extends BaseZKClient {
	
	private static final Logger log = LoggerFactory.getLogger(ManagerClient.class);
	
    private OnManagerCallback managerCallback;
    
    protected ChildrenCache workersCache;
    protected ChildrenCache workersWatcher;
    
    protected ChildrenCache workersStatusCache;
    
	/**
	 * @param zkHost
	 */
	public ManagerClient(String zkHost, OnManagerCallback cb) {
		super(zkHost);
		
		managerCallback = cb;
	}
	
	/**
     * This method creates some parent znodes we need for this example.
     * In the case the master is restarted, then this method does not
     * need to be executed a second time.
	 * @throws IOException 
	 */
	public void bootstrap() throws IOException {
		startZK();
	}
	
	@Override
	public void process(WatchedEvent e) {
		super.process(e);
		
		if (isConnected()) {
			createPath(ZKNodeConst.WORKER_PARENT_NODE, new byte[0]);
			createPath(ZKNodeConst.ASSIGN_PARENT_NODE, new byte[0]);
			createPath(ZKNodeConst.STATUS_PARENT_NODE, new byte[0]);
			createPath(ZKNodeConst.TASK_PARENT_NODE, new byte[0]);
			
			managerCallback.onConnectedSuccess();
		} else {
			managerCallback.onConnectedFailed();
		}
	}
	
    /*
     ****************************************************
     **************************************************** 
     * Methods to handle changes to the list of workers.*
     ****************************************************
     ****************************************************
     */	
	protected void getWorkers() {
		log.info("get worker node list");
		zk.getChildren(ZKNodeConst.WORKER_PARENT_NODE, workersChangeWatcher, workersGetChildrenCallback, null);
	}
	
	private Watcher workersChangeWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			if (e.getType() == EventType.NodeChildrenChanged) {
				getWorkers();
			}
		}
	};
	
	private ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getWorkers();
				break;
			case OK:
				log.info("successfully get a list of workers:{}", children.size());
				reassignAndSet(children);
				break;
			default:
				break;
			}
		}
	};
	
    private void reassignAndSet(List<String> children){
    	
        List<String> removedWorkers;
        if(workersCache == null) {
            workersCache = new ChildrenCache(children);
            removedWorkers = null;
        } else {
            removedWorkers = workersCache.removedAndSet( children );
        }
        managerCallback.onWorkersChanged(children, removedWorkers);
        
        List<String> newWorkers;
        if (workersWatcher == null) {
        	workersWatcher = new ChildrenCache(children);
        	newWorkers = children;
		} else {
			newWorkers = workersWatcher.addedAndSet(children);
		}
        if (newWorkers != null) {
			for(String worker : newWorkers) {
				watchWorkerStatus(ZKNodeConst.WORKER_PARENT_NODE + "/" + worker);
			}
		}
    }
    
    private void watchWorkerStatus(String path) {
    	log.debug("watch worker status {}", path);
    	zk.getData(path, workerDataChangedWatcher, workerGetDataCallback, path);
    }
    
    private Watcher workerDataChangedWatcher = new Watcher() {
		public void process(WatchedEvent event) {
	    	log.debug("worker node data changed, {}, {}", event.getType(), event.getPath());
			if (event.getType() == EventType.NodeDataChanged) {
				watchWorkerStatus(event.getPath());
			}
		}
	};
	
	private DataCallback workerGetDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
            	watchWorkerStatus((String) ctx);
                
                break;
            case OK:
            	log.info("worker status changed:{}, data:{}", path, data);
                
                managerCallback.onWorkerStatusChanged(path, data);
                break;
            default:
                log.error("Error when trying to get task data.", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
	};

    
    /*
     ******************************************************
     ******************************************************
     * Methods for receiving new tasks.*
     ******************************************************
     ******************************************************
     */    
    protected void getTasks(){
    	log.info("get task node list");
        zk.getChildren(ZKNodeConst.TASK_PARENT_NODE, 
                tasksChangeWatcher, 
                tasksGetChildrenCallback, 
                null);
    }
    
    private Watcher tasksChangeWatcher = new Watcher() {
    	  public void process(WatchedEvent e) {
    		  if(e.getType() == EventType.NodeChildrenChanged) {
    			  assert ZKNodeConst.TASK_PARENT_NODE.equals( e.getPath() );
    			  
    			  getTasks();
    		  }
    	  }
    };
    
    private ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();
                
                break;
            case OK:
            	log.info("get task {}, ok : {}", path, children);
                
            	managerCallback.onTaskChanged(children);
                break;
            default:
                log.error("getChildren failed.",  
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    /*
     ******************************************************
     ******************************************************
     * Methods for getting task data.*
     ******************************************************
     ******************************************************
     */    
    public void getTaskData(String task) {
    	log.info("get task data:{}", task);
        zk.getData(ZKNodeConst.TASK_PARENT_NODE + "/" + task, 
                false, 
                taskDataCallback, 
                task);
    }
    
    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTaskData((String) ctx);
                
                break;
            case OK:
            	log.info("taskDataCallback:{}, data:{}", path, data);
                
                managerCallback.onTaskData(path, ctx, data);
                break;
            default:
                log.error("Error when trying to get task data.", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    
    /*
     ******************************************************
     ******************************************************
     * Methods for assign task.*
     ******************************************************
     ******************************************************
     */ 
    public void assignTasks(String designatedWorker, String task, byte[] data) {
    	/*
    	 * Assign task to randomly chosen worker.
    	 */
    	String assignmentPath = ZKNodeConst.ASSIGN_PARENT_NODE + "/" + designatedWorker + "/" + (String) task;
    	log.info( "Assignment path: " + assignmentPath );
    	createAssignment(assignmentPath, data);	
    }
    
    private void createAssignment(String path, byte[] data){
    	log.info("assign task to worker:{}", path);
        zk.create(path, 
                data, 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                assignTaskCallback, 
                data);
    }
    
    private StringCallback assignTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                createAssignment(path, (byte[]) ctx);
                
                break;
            case OK:
                log.info("Task assigned correctly: " + name);
                deleteTask(name.substring( name.lastIndexOf("/") + 1));
                
                break;
            case NODEEXISTS: 
            	log.warn("Task already assigned");
                
                break;
            default:
            	log.error("Error when trying to assign task.", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    /*
     * Once assigned, we delete the task from /tasks
     */
    private void deleteTask(String name){
    	log.info("delete Task: {}/{}", ZKNodeConst.TASK_PARENT_NODE, name);
        zk.delete(ZKNodeConst.TASK_PARENT_NODE + "/" + name, -1, taskDeleteCallback, null);
    }
    
    VoidCallback taskDeleteCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteTask(path);
                
                break;
            case OK:
            	log.info("Successfully deleted task : " + path);
                
                break;
            case NONODE:
            	log.info("Task has been deleted already");
                
                break;
            default:
            	log.error("Something went wrong here, " + 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };	
    
    /*
     ******************************************************
     ******************************************************
     * Methods for watch task status.*
     ******************************************************
     ******************************************************
     */ 
    protected void getTasksStatus() {
    	log.debug("get tasks status list");
    	
    	zk.getChildren(ZKNodeConst.STATUS_PARENT_NODE, tasksStatusWatcher, tasksStatusCallback, null);
    }
    
    private Watcher tasksStatusWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			if (event.getType() == EventType.NodeChildrenChanged) {
				getTasksStatus();
			}
		}
	};
	
	private ChildrenCallback tasksStatusCallback = new ChildrenCallback() {
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getTasksStatus();
				break;
			case OK:
				log.debug("task status change : {}, list : {}", path, children);
				
				List<String> newWorkerStatus = null;
				if (workersStatusCache == null) {
					workersStatusCache = new ChildrenCache(children);
					newWorkerStatus = children;
				} else {
					newWorkerStatus = workersStatusCache.addedAndSet(children);
				}
				
				if (newWorkerStatus != null) {
					for(String task : newWorkerStatus) {
						watchTaskStatus(path + "/" + task); // path:/status/task-1				
					}
				}
				break;
			default:
				log.error("watch task status error : {}", KeeperException.create(Code.get(rc), path));
				break;
			}
		}
	};
	
	private void watchTaskStatus(String path) {
		log.debug("watch task status : {}", path);
		
		zk.getData(path, taskStatusWatcher, taskStatusCallback, path);
	}
	
	private Watcher taskStatusWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			if (event.getType() == EventType.NodeDataChanged) {
				watchTaskStatus(event.getPath());
			}
		}
	};
	
	private DataCallback taskStatusCallback = new DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data,	Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				watchTaskStatus(path);
				break;
			case OK:
				managerCallback.onTaskStatusChanged(path, ctx, data);
				break;
			default:
				log.error("task status change error : {}", KeeperException.create(Code.get(rc), path));
				break;
			}
		}
	};
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}
}
