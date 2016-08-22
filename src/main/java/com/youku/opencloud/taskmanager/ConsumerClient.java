/**
 * 
 */
package com.youku.opencloud.taskmanager;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.callback.OnConsumerCallback;
import com.youku.opencloud.constant.ZKNodeConst;

/**
 * @author liulietao
 *
 */
public class ConsumerClient extends BaseZKClient {

	private static final Logger log = LoggerFactory.getLogger(ConsumerClient.class);
	
	private OnConsumerCallback consumerCallback;
	
	private String serverId = Integer.toHexString((new Random()).nextInt());
	
	/**
	 * @param zkHost
	 */
	public ConsumerClient(String zkHost, OnConsumerCallback callback) {
		super(zkHost);
		
		consumerCallback = callback;
	}
	
	public void bootstrap() throws IOException {
		log.debug("bootstrap");
		startZK();
	}
	
	@Override
	public void process(WatchedEvent e) {
		super.process(e);
		
		log.debug("process, {}", e);
		
		//TODO change isConnected to isExpired function
		if (isConnected()) {
			createAssignNode();
			
			register();
			
			getTasks();
			
			consumerCallback.onConnectedSuccess();
		} else {
			consumerCallback.onConnectedFailed();
		}
	}
	
	public void close() {
		log.debug("close");
		
		try {
			stopZK();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
    /*
     *************************************** 
     ***************************************
     * Methods to create assign node 
     * of this worker.*
     *************************************** 
     ***************************************
     */
    private void createAssignNode(){
    	log.info("creating a /assign/worker-{} znode to hold the tasks assigned to this worker", serverId);
        zk.create(ZKNodeConst.ASSIGN_PARENT_NODE + "/worker-" + serverId, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                createAssignCallback, null);
    }
    
    private StringCallback createAssignCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
        	log.debug("createAssignCallback, {}, {}", Code.get(rc), path);
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
            	log.error("createAssignCallback, connection loss");
                createAssignNode();
                break;
            case OK:
                log.info("Assign node created");
                break;
            case NODEEXISTS:
                log.warn("Assign node already registered");
                break;
            default:
                log.error("Something went wrong:, {}, {}", Code.get(rc), path);
            }
        }
    };
	
    /*
     *************************************** 
     ***************************************
     * Methods to Registering the new worker
     * , which consists of adding a worker.*
     *************************************** 
     ***************************************
     */
    private String name;
    private void register(){
        name = "worker-" + serverId;
        log.info("Registering new worker, /workers/{}", name);

        //TODO : support worker node data
        zk.create(ZKNodeConst.WORKER_PARENT_NODE + "/" + name,
                "Idle".getBytes(), 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL,
                createWorkerCallback, null);
    }
    
    StringCallback createWorkerCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
        	log.debug("createWorkerCallback, {}, {}", Code.get(rc), path);
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                register();
                
                break;
            case OK:
                log.info("Registered successfully: " + serverId);
                
                break;
            case NODEEXISTS:
                log.warn("Already registered: " + serverId);
                
                break;
            default:
                log.error("Something went wrong, {}, {}", Code.get(rc), path);
            }
        }
    };
    
    /*
     *************************************** 
     ***************************************
     * Methods to wait for new assignments.*
     *************************************** 
     ***************************************
     */
    private void getTasks(){
    	log.info("getTasks, {}/worker-{}", ZKNodeConst.ASSIGN_PARENT_NODE, serverId);
    	
        zk.getChildren(ZKNodeConst.ASSIGN_PARENT_NODE + "/worker-" + serverId, 
                newTaskWatcher, 
                tasksGetChildrenCallback, 
                null);
    }
    
    private Watcher newTaskWatcher = new Watcher(){
        public void process(WatchedEvent e) {
        	log.info("newTaskWatcher:{}", e);
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert new String(ZKNodeConst.ASSIGN_PARENT_NODE + "/worker-" + serverId ).equals( e.getPath() );
                
                getTasks();
            }
        }
    };
    
    private ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			log.debug("tasksGetChildrenCallback, {}, {}", Code.get(rc), path);
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getTasks();
				break;
			case OK:
				consumerCallback.onAssignedTask(children);
				break;
			default:
				break;
			}
		}
	};
	
    /*
     *************************************** 
     ***************************************
     * Methods to update worker status.*
     *************************************** 
     ***************************************
     */
    public void setWorkerStatus(String status) {
        this.workerStatus = status;
        updateWorkerStatus(status);
    }
    
    private String workerStatus;
    synchronized private void updateWorkerStatus(String status) {
        if (status == this.workerStatus) {
        	log.info("update {} status : {}", ZKNodeConst.WORKER_PARENT_NODE + "/" + name, status);
            zk.setData(ZKNodeConst.WORKER_PARENT_NODE + "/" + name, status.getBytes(), -1,
                statusUpdateCallback, status);
        }
    }
    
    StatCallback statusUpdateCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
        	log.debug("statusUpdateCallback, {}, {}", Code.get(rc), path);
        	switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                updateWorkerStatus((String)ctx);
                return;
			default:
				break;
			}
        }
    };

    /*
     *************************************** 
     ***************************************
     * Methods to update task status.*
     *************************************** 
     ***************************************
     */    
    public void createTaskStatus(String task, String status) {
        log.info("create task status: " + status);
        
        taskMap.put(task, status);
        
        zk.create(ZKNodeConst.STATUS_PARENT_NODE + "/" + task, status.getBytes(), Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT, taskStatusCreateCallback, task);
    }
    
    protected ConcurrentHashMap<String, Object> taskMap = new ConcurrentHashMap<String, Object>();
    
    private StringCallback taskStatusCreateCallback = new StringCallback(){
        public void processResult(int rc, String path, Object ctx, String name) {
        	log.debug("set task status callback, {}, {}", Code.get(rc), path);
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
            	/*
            	 * 节点消失后，taskManager会重新下发任务
            	 */
                break;
            case OK:
                log.info("Created status znode correctly: " + name);
                break;
            case NODEEXISTS:
                log.warn("Node exists: " + path);
                break;
            default:
                log.error("Failed to create task data, {}, {}", Code.get(rc), path);
            }
            
        }
    };
    
    public void setTaskStatus(String task, String status) {
        log.info("update task status: " + status);
        
        taskMap.put(task, status);
        
        zk.setData(ZKNodeConst.STATUS_PARENT_NODE + "/" + task, status.getBytes(), -1, taskStatusUpdateCallback, task);
    }
    
    private StatCallback taskStatusUpdateCallback = new StatCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			log.debug("task status update callback, {}, {}", Code.get(rc), path);
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				break;
			case OK:
				break;
			default:
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
