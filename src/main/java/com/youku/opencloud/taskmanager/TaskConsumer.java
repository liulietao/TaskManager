/**
 * 
 */
package com.youku.opencloud.taskmanager;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;



import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.constant.ZKNodeConst;
import com.youku.opencloud.util.GzipUtil;

/**
 * @author liulietao
 *
 */
public class TaskConsumer implements Watcher, Closeable {

    private static final Logger log = LoggerFactory.getLogger(TaskConsumer.class);
    
    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString((new Random()).nextInt());
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    
    /*
     * In general, it is not a good idea to block the callback thread
     * of the ZooKeeper client. We use a thread pool executor to detach
     * the computation from the callback.
     */
    private ThreadPoolExecutor executor;
    
    public TaskConsumer(String hostPort) {
        this.hostPort = hostPort;
        this.executor = new ThreadPoolExecutor(8, 8, 
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(100),
                new ThreadPoolExecutor.CallerRunsPolicy());
	}
    
    /**
     * Creates a ZooKeeper session.
     * 
     * @throws IOException
     */
    public void startZK() throws IOException {
    	log.info("Creates a ZooKeeper session");
        zk = new ZooKeeper(hostPort, 15000, this);
    }
    
    /**
     * Deals with session events like connecting
     * and disconnecting.
     * 
     * @param e new event generated
     */
    public void process(WatchedEvent e) { 
        log.info(e.toString() + ", " + hostPort);
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
            case SyncConnected:
                /*
                 * Registered with ZooKeeper
                 */
                connected = true;
                break;
            case Disconnected:
                connected = false;
                break;
            case Expired:
                expired = true;
                connected = false;
                log.error("Session expired");
            default:
                break;
            }
        }
    }
    
    /**
     * Checks if this client is connected.
     * 
     * @return boolean
     */
    public boolean isConnected() {
        return connected;
    }
    
    /**
     * Checks if ZooKeeper session is expired.
     * 
     * @return
     */
    public boolean isExpired() {
        return expired;
    }
    
    /**
     * Bootstrapping here is just creating a /assign parent
     * znode to hold the tasks assigned to this worker.
     */
    public void bootstrap(){
    	log.info("bootstrap");
        createAssignNode();
    }
    
    void createAssignNode(){
    	log.info("creating a {}/worker-serverId parent znode to hold the tasks assigned to this worker", ZKNodeConst.ASSIGN_PARENT_NODE);
        zk.create(ZKNodeConst.ASSIGN_PARENT_NODE + "/worker-" + serverId, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                createAssignCallback, null);
    }
    
    StringCallback createAssignCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
                createAssignNode();
                break;
            case OK:
                log.info("Assign node created");
                break;
            case NODEEXISTS:
                log.warn("Assign node already registered");
                break;
            default:
                log.error("Something went wrong, {}, {}", Code.get(rc), path);
            }
        }
    };
    
    String name;

    /**
     * Registering the new worker, which consists of adding a worker
     * znode to /workers.
     */
    public void register(){
        name = "worker-" + serverId;
        log.info("Registering the new worker, {}/{}", ZKNodeConst.WORKER_PARENT_NODE, name);
        
        byte[] data;
		try {
			data = GzipUtil.gzip("Idle".getBytes());
	        zk.create(ZKNodeConst.WORKER_PARENT_NODE + "/" + name,
	        		data, 
	                Ids.OPEN_ACL_UNSAFE, 
	                CreateMode.EPHEMERAL,
	                createWorkerCallback, null);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("register, " + e);
		}
    }
    
    StringCallback createWorkerCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
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
    
    StatCallback statusUpdateCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(Code.get(rc)) {
	            case CONNECTIONLOSS:
	                updateStatus((String)ctx);
	                return;
            }
        }
    };
    
    String status;
    synchronized private void updateStatus(String status) {
        if (status.equals(this.status)) {
        	log.info("updateStatus, {}", status);
        	
        	byte[] data;
			try {
				data = GzipUtil.gzip(status.getBytes());
				
	            zk.setData(ZKNodeConst.WORKER_PARENT_NODE + "/" + name, data, -1,
	                    statusUpdateCallback, status);
			} catch (Exception e) {
				e.printStackTrace();
				log.error("updateStatus, " + e);
			}
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }
    
    private int executionCount;

    synchronized void changeExecutionCount(int countChange) {
        executionCount += countChange;
        log.info("changeExecutionCount, {}", executionCount);
        if (executionCount == 0 && countChange < 0) {
            // we have just become idle
            setStatus("Idle");
        }
        if (executionCount == 1 && countChange > 0) {
            // we have just become idle
            setStatus("Working");
        }
    }
    /*
     *************************************** 
     ***************************************
     * Methods to wait for new assignments.*
     *************************************** 
     ***************************************
     */
    
    Watcher newTaskWatcher = new Watcher(){
        public void process(WatchedEvent e) {
        	log.info("newTaskWatcher:{}", e);
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert new String(ZKNodeConst.ASSIGN_PARENT_NODE + "/worker-"+ serverId ).equals( e.getPath() );
                
                getTasks();
            }
        }
    };
    
    void getTasks(){
    	log.info("getTasks, {}/worker-{}", ZKNodeConst.ASSIGN_PARENT_NODE, serverId);
    	
        zk.getChildren(ZKNodeConst.ASSIGN_PARENT_NODE + "/worker-" + serverId, 
                newTaskWatcher, 
                tasksGetChildrenCallback, 
                null);
    }
    
   
    protected ChildrenCache assignedTasksCache = new ChildrenCache();
    
    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                if(children != null){
                    executor.execute(new Runnable() {
                        List<String> children;
                        DataCallback cb;
                        
                        /*
                         * Initializes input of anonymous class
                         */
                        public Runnable init (List<String> children, DataCallback cb) {
                            this.children = children;
                            this.cb = cb;
                            
                            return this;
                        }
                        
                        public void run() {
                            if(children == null) {
                                return;
                            }
    
                            log.info("Looping into tasks");
                            setStatus("Working");
                            for(String task : children){
                                log.trace("New task: {}", task);
                                zk.getData(ZKNodeConst.ASSIGN_PARENT_NODE + "/worker-" + serverId  + "/" + task,
                                        false,
                                        cb,
                                        task);   
                            }
                        }
                    }.init(assignedTasksCache.addedAndSet(children), taskDataCallback));
                } 
                break;
            default:
                log.debug("getChildren failed, {}, {}", Code.get(rc), path);
            }
        }
    };
    
    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                zk.getData(path, false, taskDataCallback, null);
                break;
            case OK:
                /*
                 *  Executing a task in this example is simply printing out
                 *  some string representing the task.
                 */
                executor.execute( new Runnable() {
                    byte[] data;
                    Object ctx;
                    
                    /*
                     * Initializes the variables this anonymous class needs
                     */
                    public Runnable init(byte[] data, Object ctx) {
                        try {
							this.data = GzipUtil.ungzip(data);
						} catch (Exception e) {
							e.printStackTrace();
						}
                        
                        this.ctx = ctx;
                        
                        return this;
                    }
                    
                    public void run() {
                        log.info("Executing your task: " + new String(data));
                        int waitTime = 0;
                        try {
                        	while (waitTime < 60) {
								System.out.print(".");
								Thread.sleep(1000);
								waitTime ++;
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
                        log.info("finish task: " + new String(data));
                        
                        byte[] nodeData;
						try {
							nodeData = GzipUtil.gzip("done".getBytes());
							zk.create(ZKNodeConst.STATUS_PARENT_NODE + "/" + (String) ctx, nodeData, Ids.OPEN_ACL_UNSAFE, 
									CreateMode.PERSISTENT, taskStatusCreateCallback, nodeData);
						} catch (Exception e) {
							e.printStackTrace();
							log.error("taskDataCallback, " + e);
						}
                        
                        log.info("delete assign worker: /assign/worker-" + serverId + "/" + (String) ctx);
                        zk.delete(ZKNodeConst.ASSIGN_PARENT_NODE + "/worker-" + serverId + "/" + (String) ctx, 
                                -1, taskVoidCallback, null);
                    }
                }.init(data, ctx));
                
                break;
            default:
                log.error("Failed to get task data, {}, {}", Code.get(rc), path);
            }
        }
    };
    
    StringCallback taskStatusCreateCallback = new StringCallback(){
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                zk.create(path + ZKNodeConst.STATUS_PARENT_NODE, (byte[])ctx, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                        taskStatusCreateCallback, null);
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
    
    VoidCallback taskVoidCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                break;
            case OK:
                log.info("Task correctly deleted: " + path);
                break;
            default:
                log.error("Failed to delete task data, {}, {}", Code.get(rc), path);
            } 
        }
    };
    
    /**
     * Closes the ZooKeeper session.
     */
    @Override
    public void close() 
            throws IOException
    {
        log.info( "Closing" );
        try{
            zk.close();
        } catch (InterruptedException e) {
            log.warn("ZooKeeper interrupted while closing");
        }
    }

	/**
	 * @param args
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {
        TaskConsumer w = new TaskConsumer(args[0]);
        w.startZK();
        
        while(!w.isConnected()){
            Thread.sleep(100);
        }   
        /*
         * bootstrap() create some necessary znodes.
         */
        w.bootstrap();
        
        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
        w.register();
        
        /*
         * Getting assigned tasks.
         */
        w.getTasks();
        
        while(!w.isExpired()){
            Thread.sleep(1000);
        }

	}

}
