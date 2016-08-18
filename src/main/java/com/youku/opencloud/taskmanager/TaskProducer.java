/**
 * 
 */
package com.youku.opencloud.taskmanager;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.*;

import com.youku.opencloud.taskmanager.TaskRecover.RecoveryCallback;
import com.youku.opencloud.util.OSUtils;

/**
 * @author liulietao
 *
 */
public class TaskProducer implements Watcher, Closeable {
	
	private static final Logger log = LoggerFactory.getLogger(TaskProducer.class);
	
	enum MasterStates {RUNNING, ELECTED, NOTELECTED};
	private volatile MasterStates state = MasterStates.RUNNING;
	
	private Random random = new Random(this.hashCode());
	private String serverId = Integer.toHexString( random.nextInt() );
	
	private ZooKeeper zk;
	private String hostPort;
	
	private volatile boolean connected = false;
	private volatile boolean expired = false;
	
    protected ChildrenCache tasksCache;
    protected ChildrenCache workersCache;

	/**
	 * Create a new task producer
	 * 
	 */
	TaskProducer(String hostPort) {
		this.hostPort = hostPort;
	}
	
	void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 3000, this);
	}
	
	void stopZK() throws IOException, InterruptedException {
		zk.close();
	}
	
    /**
     * Check if this client is connected.
     * 
     * @return boolean ZooKeeper client is connected
     */
    boolean isConnected() {
        return connected;
    }
    
    /**
     * Check if the ZooKeeper session has expired.
     * 
     * @return boolean ZooKeeper session has expired
     */
    boolean isExpired() {
        return expired;
    }	
	
	/* (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		log.info("close");
	}

	/* (non-Javadoc)
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(WatchedEvent e) {
		log.info("process event, type:{}, state:{}", e.getType(), e.getState());
		
		if (e.getType() == Event.EventType.None) {
			switch (e.getState()) {
			case SyncConnected:
				connected = true;
				break;
			case Disconnected:
				connected = false;
				break;
			case Expired:
				expired = true;
				connected = false;
				log.error("session expired");
				break;
			default:
				break;
			}
		}
	}
	
	/**
     * This method creates some parent znodes we need for this example.
     * In the case the master is restarted, then this method does not
     * need to be executed a second time.
	 */
	public void bootstrap() {
		createParent("/workers", new byte[0]);
		createParent("/assign", new byte[0]);
		createParent("/status", new byte[0]);
		createParent("/tasks", new byte[0]);
	}

	private void createParent(String path, byte[] data) {
		log.info("createParent:{}", path);
		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
	}
	
	private StringCallback createParentCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
                createParent(path, (byte[]) ctx);
				break;
			case OK:
				log.info("parent created {}", path);
				break;
			case NODEEXISTS:
				log.info("parent already exist {}", path);
				break;
			default:
				log.error("error:{}", KeeperException.create(Code.get(rc), path));
				break;
			}
		}
	};
	
	
    /*
     **************************************
     **************************************
     * Methods related to master election.*
     **************************************
     **************************************
     */
	
    /*
     * The story in this callback implementation is the following.
     * We tried to create the master lock znode. If it suceeds, then
     * great, it takes leadership. However, there are a couple of
     * exceptional situations we need to take care of. 
     * 
     * First, we could get a connection loss event before getting
     * an answer so we are left wondering if the operation went through.
     * To check, we try to read the /master znode. If it is there, then
     * we check if this master is the primary. If not, we run for master
     * again. 
     * 
     *  The second case is if we find that the node is already there.
     *  In this case, we call exists to set a watch on the znode.
     */
	public void runForMaster() {
		log.info("running for master");
		zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL
				, masterCreateCallback, null);
	}
	
	private StringCallback masterCreateCallback = new  StringCallback() {
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case OK:
				state = MasterStates.ELECTED;
				takeLeadership();
				break;
			case NODEEXISTS:
				state = MasterStates.NOTELECTED;
				masterExists();
				break;
			default:
				state = MasterStates.NOTELECTED;
				log.error("Something went wrong when running for master, {}", 
						KeeperException.create(Code.get(rc), path));
				break;
			}
			log.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
		}
	};
	
	private void takeLeadership() {
		log.info("take leader, get worker, get task");
		getWorkers();
		
		
        (new TaskRecover(zk)).recover( new RecoveryCallback() {
            public void recoveryComplete(int rc, List<String> tasks) {
                if(rc == RecoveryCallback.FAILED) {
                    log.error("Recovery of assigned tasks failed.");
                } else {
                    log.info( "Assigning recovered tasks" );
                    getTasks();
                }
            }
        });
	}
	
	private void masterExists() {
		log.info("watch on master node");
		zk.exists("/master", masterExistsWatcher, masterExistsCallback, null);
	}
	
	StatCallback masterExistsCallback = new StatCallback() {
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				masterExists();
				break;
			case OK:
				break;
			case NONODE:
				state = MasterStates.RUNNING;
				runForMaster();
				log.info("It seems master is gone, so let's run for master");
				break;
			default:
				checkMaster();
				break;
			}
		}
	};
	
	Watcher masterExistsWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			if (e.getType() == EventType.NodeDeleted) {
				runForMaster();
			}
		}
	};
	
	private DataCallback masterCheckCallback = new DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case NONODE:
				runForMaster();
				break;
			case OK:
				if ( serverId.equals( new String(data) )) {
					state = MasterStates.ELECTED;
					takeLeadership();
				} else {
					state = MasterStates.NOTELECTED;
					masterExists();
				}
				break;
			default:
				log.error("error when check master, {}", KeeperException.create(Code.get(rc)));
				break;
			}
		}
	};
	
	private void checkMaster() {
		log.info("check master node");
		zk.getData("/master", false, masterCheckCallback, null);
	}
	
    /*
     ****************************************************
     **************************************************** 
     * Methods to handle changes to the list of workers.*
     ****************************************************
     ****************************************************
     */	
	
	public int  getWorkersSize() {
		if (workersCache == null) {
			return 0;
		} else {
			return workersCache.getList().size();
		}
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
	
	private void getWorkers() {
		log.info("get worker node list");
		zk.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null);
	}
	
    /*
     *******************
     *******************
     * Assigning tasks.*
     *******************
     *******************
     */
    void reassignAndSet(List<String> children){
        List<String> toProcess;
        
        log.info("workers list:{}", children);
        if(workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            log.info( "Removing and setting : {}" , children);
            toProcess = workersCache.removedAndSet( children );
        }
        
        if(toProcess != null) {
            for(String worker : toProcess){
                getAbsentWorkerTasks(worker);
            }
        }
    }
    
    void getAbsentWorkerTasks(String worker){
    	log.info("get absent worker task");
        zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }
    
    ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getAbsentWorkerTasks(path);
                
                break;
            case OK:
                log.info("Succesfully got a list of assignments: "  + children.size()  + " tasks");
                
                /*
                 * Reassign the tasks of the absent worker.  
                 */
                
                for(String task: children) {
                    getDataReassign(path + "/" + task, task);                    
                }
                break;
            default:
                log.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
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
    class RecreateTaskCtx {
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
                recreateTask(new RecreateTaskCtx(path, (String) ctx, data));
                
                break;
            default:
                log.error("Something went wrong when getting data ",
                        KeeperException.create(Code.get(rc)));
            }
        }
    };
    
    /**
     * Recreate task znode in /tasks
     * 
     * @param ctx Recreate text context
     */
    void recreateTask(RecreateTaskCtx ctx) {
    	log.info("Recreate task znode in /tasks : {}", ctx.task);
        zk.create("/tasks/" + ctx.task,
                ctx.data,
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                recreateTaskCallback,
                ctx);
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
                log.error("Something wwnt wrong when recreating task", 
                        KeeperException.create(Code.get(rc)));
            }
        }
    };
    
    /**
     * Delete assignment of absent worker
     * 
     * @param path Path of znode to be deleted
     */
    void deleteAssignment(String path){
    	log.info("Delete assignment of absent worker");
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
                log.error("Failed to delete task data" + 
                        KeeperException.create(Code.get(rc), path));
            } 
        }
    };
    
    /*
     ******************************************************
     ******************************************************
     * Methods for receiving new tasks and assigning them.*
     ******************************************************
     ******************************************************
     */
      
    Watcher tasksChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/tasks".equals( e.getPath() );
                
                getTasks();
            }
        }
    };
    
    void getTasks(){
    	log.info("get task node list");
        zk.getChildren("/tasks", 
                tasksChangeWatcher, 
                tasksGetChildrenCallback, 
                null);
    }
    
    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                
                break;
            case OK:
            	log.info("get task {} ok : {}", path, children);
                List<String> toProcess;
                if(tasksCache == null) {
                    tasksCache = new ChildrenCache(children);
                    
                    toProcess = children;
                } else {
                    toProcess = tasksCache.addedAndSet( children );
                }
                
                if(toProcess != null){
                    assignTasks(toProcess);
                } 
                
                break;
            default:
                log.error("getChildren failed.",  
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    void assignTasks(List<String> tasks) {
    	log.info("assign task");
        for(String task : tasks){
            getTaskData(task);
        }
    }

    void getTaskData(String task) {
    	log.info("get task data:{}", task);
        zk.getData("/tasks/" + task, 
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
            	log.info("taskDataCallback ok");
                /*
                 * Choose worker at random.
                 */
                List<String> list = workersCache.getList();
                if (list.size() > 0) {
                	String designatedWorker = list.get(random.nextInt(list.size()));
                	
                	/*
                	 * Assign task to randomly chosen worker.
                	 */
                	String assignmentPath = "/assign/" + designatedWorker + "/" + (String) ctx;
                	log.info( "Assignment path: " + assignmentPath );
                	createAssignment(assignmentPath, data);					
				}
                
                break;
            default:
                log.error("Error when trying to get task data.", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    void createAssignment(String path, byte[] data){
    	log.info("createAssignment:{}", path);
        zk.create(path, 
                data, 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                assignTaskCallback, 
                data);
    }
    
    StringCallback assignTaskCallback = new StringCallback() {
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
    void deleteTask(String name){
    	log.info("deleteTask: /tasks/{}", name);
        zk.delete("/tasks/" + name, -1, taskDeleteCallback, null);
    }
    
    VoidCallback taskDeleteCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteTask(path);
                
                break;
            case OK:
            	log.info("Successfully deleted " + path);
                
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
     *******************
     *******************
     * Test Main.*
     *******************
     *******************
     */
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException, InterruptedException {
		TaskProducer p = new TaskProducer(args[0]);
		p.startZK();
		
		while (! p.isConnected()) {
			Thread.sleep(100);
		}
		
		p.bootstrap();
		
		p.runForMaster();
		
        while(!p.isExpired()){
            Thread.sleep(1000);
        }   
		
		int cpuLoad = OSUtils.cpuUsage();
		log.info("cpu load : {}", cpuLoad);
		
		p.stopZK();
	}

}
