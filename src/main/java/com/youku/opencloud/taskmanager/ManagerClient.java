/**
 * 
 */
package com.youku.opencloud.taskmanager;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
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
import com.youku.opencloud.util.GzipUtil;

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
    
    protected ChildrenCache tasksCache;
    
    private ThreadPoolExecutor executor;
    
	/**
	 * @param zkHost
	 */
	public ManagerClient(String zkHost, OnManagerCallback cb) {
		super(zkHost);
		
		managerCallback = cb;
		
        this.executor = new ThreadPoolExecutor(8, 8, 
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(100),
                new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	/**
     * This method creates some parent znodes we need for this example.
     * In the case the master is restarted, then this method does not
     * need to be executed a second time.
	 * @throws IOException 
	 */
	public void bootstrap() throws IOException {
		log.info("bootstrap");
		startZK();
	}
	
	@Override
	public void process(WatchedEvent e) {
		super.process(e);
		
		log.info("");
		
		if (isConnected()) {
			createPath(ZKNodeConst.WORKER_PARENT_NODE, new byte[0]);
			createPath(ZKNodeConst.ASSIGN_PARENT_NODE, new byte[0]);
			createPath(ZKNodeConst.STATUS_PARENT_NODE, new byte[0]);
			createPath(ZKNodeConst.TASK_PARENT_NODE, new byte[0]);
		}
		
		if (isExpired()) {
			managerCallback.onSessionExpired();
		} else {
			managerCallback.onSessionStart();
		}
	}
	
	public void close() {
		try {
			log.info("");
			stopZK();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
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
				log.info("workersGetChildrenCallback, successfully get a list of workers:{}", children);
				
				executor.execute(new Runnable() {
					List<String> workers;
					
					public Runnable init (List<String> workers) {
						this.workers = workers;
						
						return this;
					}
					
					public void run() {
						if(workers == null) {
							return;
						}
						
						log.info("workersGetChildrenCallback, deal with workers in thread:{}", workers);
						
						reassignAndSet(workers);
					}
				}.init(children));
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
        
        List<String> newWorkers;
        if (workersWatcher == null) {
        	workersWatcher = new ChildrenCache(children);
        	newWorkers = children;
		} else {
			newWorkers = workersWatcher.addedAndSet(children);
		}
        
        managerCallback.onWorkersChanged(newWorkers, removedWorkers);
        
        // new workers should be watched
        if (newWorkers != null) {
			for(String worker : newWorkers) {
				watchWorkerStatus(ZKNodeConst.WORKER_PARENT_NODE + "/" + worker);
			}
		}
        
        // tasks should be reCreate which belong to removed workers 
        if(removedWorkers != null) {
            for(String worker : removedWorkers){
                getAbsentWorkerTasks(worker);
            }
        }
    }
    
    private void watchWorkerStatus(String path) {
    	log.info("watch worker status, path:{}", path);
    	zk.getData(path, workerDataChangedWatcher, workerGetDataCallback, path);
    }
    
    private Watcher workerDataChangedWatcher = new Watcher() {
		public void process(WatchedEvent event) {
	    	log.info("workerDataChangedWatcher, code:{}, path:{}", event.getType(), event.getPath());
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
            	log.info("worker status changed, path:{}", path);
            	
				executor.execute(new Runnable() {
					String path;
					byte[] data;
					
					public Runnable init (String path, byte[] data) {
						this.path = path;
						this.data = data;
						
						return this;
					}
					
					public void run() {
		            	byte[] nodeData;
						try {
							nodeData = GzipUtil.ungzip(data);
							managerCallback.onWorkerStatusChanged(path.substring(path.lastIndexOf('/') + 1), nodeData);
						} catch (Exception e) {
							e.printStackTrace();
							log.error("workerGetDataCallback, " + e);
						}
					}
				}.init(path, data));
                break;
            default:
                log.error("Error when trying to get task data, {}, {}", Code.get(rc), path);
            }
        }
	};

    void getAbsentWorkerTasks(String worker){
    	log.info("get absent worker task, {}", worker);
        zk.getChildren(ZKNodeConst.ASSIGN_PARENT_NODE + "/" + worker, false, workerAssignmentCallback, null);
    }
    
    ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getAbsentWorkerTasks(path);
                
                break;
            case OK:
                log.info("Succesfully got a list of assignments {} tasks, path:{}", children.size(), path);
                
                if (children.size() <= 0) {
					deleteAssignment(path);
				}
                
                /*
                 * Reassign the tasks of the absent worker.  
                 */
                
                for(String task: children) {
                    getDataReassign(path + "/" + task, task);                    
                }
                break;
            default:
                log.error("getChildren failed, {}, {}", Code.get(rc), path);
            }
        }
    };	
	
    /*
     ************************************************
     * Recovery of tasks assigned to absent worker. * 
     ************************************************
     */
    
    /**
     * Get reassigned task data.
     * 
     * @param path Path of assigned task
     * @param task Task name excluding the path prefix
     */
    void getDataReassign(String path, String task) {
    	log.info("Get reassigned task data:{}, {}", path, task);
        zk.getData(path, 
                false, 
                getDataReassignCallback, 
                task);
    }
    
    /**
     * Context for recreate operation.
     *
     */
    public class RecreateTaskCtx {
        String path; 
        String task;
        byte[] data;
        
        RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }

    /**
     * Get task data reassign callback.
     */
    DataCallback getDataReassignCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getDataReassign(path, (String) ctx); 
                
                break;
            case OK:
            	
            	byte[] nodeData;
				try {
					nodeData = GzipUtil.ungzip(data);
					recreateTask(new RecreateTaskCtx(path, (String) ctx, nodeData));
				} catch (Exception e) {
					e.printStackTrace();
				}
                
                break;
            default:
                log.error("Something went wrong when getting data, {}, {}", Code.get(rc), path);
            }
        }
    };
    
    /**
     * Recreate task znode in /tasks
     * 
     * @param ctx Recreate text context
     */
    void recreateTask(RecreateTaskCtx ctx) {
    	log.info("recreateTask, task znode in /tasks : {}", ctx.task);
    	
    	try {
			byte[] data = GzipUtil.gzip(ctx.data);
			zk.create("/tasks/" + ctx.task,
					data,
					Ids.OPEN_ACL_UNSAFE, 
					CreateMode.PERSISTENT,
					recreateTaskCallback,
					ctx);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("recreateTask, " + e);
		}
    }
    
    /**
     * Recreate znode callback
     */
    StringCallback recreateTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                recreateTask((RecreateTaskCtx) ctx);
       
                break;
            case OK:
                deleteAssignment(((RecreateTaskCtx) ctx).path);
                
                break;
            case NODEEXISTS:
                log.info("Node exists already, but if it hasn't been deleted, " +
                		"then it will eventually, so we keep trying: " + path);
                recreateTask((RecreateTaskCtx) ctx);
                
                break;
            default:
                log.error("Something wwnt wrong when recreating task, {}, {}", Code.get(rc), path);
            }
        }
    };
    
    /**
     * Delete assignment of absent worker
     * 
     * @param path Path of znode to be deleted
     */
    void deleteAssignment(String path){
    	log.info("deleteAssignment, delete assignment of absent worker, {}", path);
        zk.delete(path, -1, taskDeletionCallback, null);
    }
    
    VoidCallback taskDeletionCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteAssignment(path);
                break;
            case OK:
                log.info("Task correctly deleted: " + path);
                break;
            default:
                log.error("Failed to delete task data, {}, {}", Code.get(rc), path);
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
        	log.info("tasksGetChildrenCallback, code:" + Code.get(rc) + ", path {}, tasks {}", path, children);
        	
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                List<String> newTasks;
                if(tasksCache == null) {
                    tasksCache = new ChildrenCache(children);
                    newTasks = children;
                } else {
                    newTasks = tasksCache.addedAndSet( children );
                }
                if (newTasks != null) {
                	for(String task : newTasks){
                		getTaskData(task);
                	}					
				}
                
                break;
            default:
                log.error("getChildren failed, {}, {}", Code.get(rc), path);
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
    private void getTaskData(String task) {
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
            	log.info("taskDataCallback:{}", path);
            	
				executor.execute(new Runnable() {
					Object ctx;
					byte[] data;
					
					public Runnable init (Object ctx, byte[] data) {
						this.ctx  = ctx;
						this.data = data;
						
						return this;
					}
					
					public void run() {
		            	try {
							byte[] taskData = GzipUtil.ungzip(data);
							managerCallback.onTaskChanged((String)ctx, taskData);
						} catch (Exception e) {
							e.printStackTrace();
							log.error("taskDataCallback, " + e);
						}
					}
				}.init(ctx, data));
                
                break;
            default:
                log.error("Error when trying to get task data, {}, {}", Code.get(rc), path);
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
    public void assignTasks(String worker, String task, byte[] data) {
    	log.info("assignTasks, worker:{}, task:{}", worker, task);
    	if(tasksCache.contains(task) && workersCache.contains(worker)) {
    		String assignmentPath = ZKNodeConst.ASSIGN_PARENT_NODE + "/" + worker + "/" + (String) task;
    		log.info( "Assignment path: " + assignmentPath );
    		createAssignment(assignmentPath, data);	    		
    	}
    }
    
    private void createAssignment(String path, byte[] data){
    	log.info("createAssignment, assign task to worker:{}", path);
    	
    	try {
			byte[] taskData = GzipUtil.gzip(data);
	        zk.create(path, 
	        		taskData, 
	                Ids.OPEN_ACL_UNSAFE, 
	                CreateMode.PERSISTENT,
	                assignTaskCallback, 
	                data);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("createAssignment, " + e);
		}
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
            	log.error("Error when trying to assign task, {}, {}", Code.get(rc), path);
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
            	log.error("Something went wrong here, {}, {}", Code.get(rc), path);
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
    	log.info("getTasksStatus, " + ZKNodeConst.STATUS_PARENT_NODE);
    	
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
				log.info("tasksStatusCallback, task status change : {}, list : {}", path, children);
				
				List<String> newWorkerStatus = null;
				if (workersStatusCache == null) {
					workersStatusCache = new ChildrenCache(children);
					newWorkerStatus = children;
				} else {
					newWorkerStatus = workersStatusCache.addedAndSet(children);
				}
				
				if (newWorkerStatus != null) {
					for(String task : newWorkerStatus) {
						watchTaskStatus(path + "/" + task); //  {/status/task-1}				
					}
				}
				break;
			default:
				log.error("watch task status error, {}, {}", Code.get(rc), path);
				break;
			}
		}
	};
	
	private void watchTaskStatus(String path) {
		log.info("watch task status : {}", path);
		
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
				String taskName = path.substring(path.lastIndexOf('/') + 1);
				
				executor.execute(new Runnable() {
					String taskName;
					byte[] data;
					
					public Runnable init (String taskName, byte[] data) {
						this.taskName = taskName;
						this.data = data;
						
						return this;
					}
					
					public void run() {
						if(taskName == null) {
							return;
						}
						
						byte[] nodeData;
						try {
							nodeData = GzipUtil.ungzip(data);
							log.info("taskStatusCallback, task:{}, datalen:{}", taskName, nodeData.length);
							managerCallback.onTaskStatusChanged(taskName, nodeData);
						} catch (Exception e) {
							e.printStackTrace();
							log.error("taskStatusCallback, " + e);
						}
					}
				}.init(taskName, data));
				break;
			default:
				log.error("task status change error, {}, {}", Code.get(rc), path);
				break;
			}
		}
	};
	
    /*
     ******************************************************
     ******************************************************
     * Methods for process task status result.*
     ******************************************************
     ******************************************************
     */ 
	public void deleteTaskStatus(String taskName) {		
		String path = ZKNodeConst.STATUS_PARENT_NODE + "/" + taskName;
		log.info("deleteTaskStatus, {}", path);
		zk.delete(path, -1, deleteTaskStatusCallback, taskName);
	}
	
	private VoidCallback deleteTaskStatusCallback = new VoidCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				deleteTaskStatus(path);
				break;
			case OK:
				log.debug("delete task stauts ok, {}", path);
				break;
			default:
				log.error("deleteTaskStatusCallback, something went wrong here, {}, {}", Code.get(rc), path);
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
