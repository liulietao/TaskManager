/**
 * 
 */
package com.youku.cloud.taskmanager.client;

import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONObject;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
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

import com.youku.cloud.taskmanager.callback.OnConsumerCallback;
import com.youku.cloud.taskmanager.constant.ZKNodeConst;
import com.youku.cloud.taskmanager.dto.WorkerStatusDto;
import com.youku.cloud.taskmanager.util.GzipUtil;
import com.youku.cloud.taskmanager.util.OSUtils;

/**
 * @author liulietao
 *
 */
public class ConsumerClient extends BaseZKClient {

	private static final Logger log = LoggerFactory.getLogger(ConsumerClient.class);
	
	private OnConsumerCallback consumerCallback;
	
	private String serverId = Integer.toHexString((new Random()).nextInt());
	
	protected ChildrenCache tasksCache;
	protected ChildrenCache tasksWatcher;
	
	private ThreadPoolExecutor executor;
	
	private byte[] workerData;// worker describe
	
	private boolean isRegistered = false;
	/**
	 * @param zkHost
	 */
	public ConsumerClient(String zkHost, OnConsumerCallback callback) {
		super(zkHost);
		
		consumerCallback = callback;
		
		int coreNum = Runtime.getRuntime().availableProcessors();
		
		log.info("ConsumerClient, core : {}", coreNum);
		
        this.executor = new ThreadPoolExecutor(coreNum * 3, coreNum * 5, 
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());
        
        serverId = UUID.randomUUID().toString();
	}
	
	public void bootstrap(byte[] workerDescribe) throws IOException {
		log.info("bootstrap");
		startZK();
		
		this.workerData = workerDescribe;
	}
	
	@Override
	public void process(WatchedEvent e) {
		super.process(e);
		
		log.info("process, {}", e);
		
		if (isConnected() && !isRegistered) {
			createAssignNode();
			register();
			getTasks();
			updateSysLoad();
		}
		
		if (isExpired()) {
			isRegistered = false;
			consumerCallback.onSessionExpired();
		} else if(isConnected()){
			isRegistered = true;
			consumerCallback.onSessionStart();
		}
	}
	
	public void close() {
		log.info("close");
		
		try {
			stopZK();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
    /*
     ************************************************
     ************************************************
     * Methods to create assign node of this worker.*
     ************************************************
     ************************************************
     */
    private void createAssignNode(){
    	log.info("createAssignNode, creating a {}/worker-{} znode to hold the tasks assigned to this worker", ZKNodeConst.ASSIGN_PARENT_NODE, serverId);
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
                log.info("createAssignCallback, Assign node created");
                
                
                break;
            case NODEEXISTS:
                log.warn("createAssignCallback, Assign node already registered");
                
                break;
            default:
                log.error("createAssignCallback, Something went wrong:, {}, {}", Code.get(rc), path);
            }
        }
    };
    
    public void deleteAssignTask(String taskName) {
    	String path = ZKNodeConst.ASSIGN_PARENT_NODE + "/worker-" + serverId + "/" + taskName;
    	log.info("deleteAssignNode, delete {}", path);
    	
        zk.delete(path, -1, taskDeletionCallback, null);
    }
    
    VoidCallback taskDeletionCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
            	deleteAssignTask(path);
                break;
            case OK:
                log.info("taskDeletionCallback, Task correctly deleted: " + path);
                break;
            default:
                log.error("taskDeletionCallback, failed to delete task data, {}, {}", Code.get(rc), path);
            } 
        }
    };
	
    /*
     ****************************************************************************
     ****************************************************************************
     * Methods to Registering the new worker, which consists of adding a worker.*
     ****************************************************************************
     ****************************************************************************
     */
    private String name;
    private void register(){
        name = "worker-" + serverId;
        log.info("register, Registering new worker, {}/{}", ZKNodeConst.WORKER_PARENT_NODE, name);

		WorkerStatusDto workerStatus = new WorkerStatusDto();
		workerStatus.setData(this.workerData);
		workerStatus.setLoad(getSysLoad());
		workerStatus.setCpuCore(getCpuCore());
		workerStatus.setIp(getIP());
		
		JSONObject workerStatusJson = JSONObject.fromObject(workerStatus);
		
		byte[] data;
		try {
			data = GzipUtil.gzip(workerStatusJson.toString().getBytes());
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
        	log.info("createWorkerCallback, {}, {}", Code.get(rc), path);
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                register();
                
                break;
            case OK:
                log.info("Registered successfully: " + name);
                
                break;
            case NODEEXISTS:
                log.warn("Already registered: " + name);
                
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
			log.info("tasksGetChildrenCallback, {}, path:{}", Code.get(rc), path);
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getTasks();
				break;
			case OK:
				List<String> newTasks;
				if (tasksWatcher == null) {
					tasksWatcher = new ChildrenCache(children);
					newTasks = children;
				} else {
					newTasks = tasksWatcher.addedAndSet(children);
				}
				log.info("tasksGetChildrenCallback, newTasks:{}", newTasks);
				if (newTasks != null) {
					executor.execute(new Runnable() {
						List<String> children;
						DataCallback cb;
						
						public Runnable init (List<String> children, DataCallback cb) {
							this.children = children;
							this.cb = cb;
							
							return this;
						}
						
						public void run() {
							log.info("tasksGetChildrenCallback, Runnable run to get tasks data:{}", children);
							if(children == null) {
								return;
							}

							for(String task : children){
								zk.getData(ZKNodeConst.ASSIGN_PARENT_NODE + "/worker-" + serverId  + "/" + task, false, cb, task);   
							}
						}
					}.init(newTasks, taskDataCallback));
				}
				
				List<String> removedTasks;
				if (tasksCache == null) {
					tasksCache = new ChildrenCache(children);
					removedTasks = null;
				} else {
					removedTasks = tasksCache.removedAndSet(children);
				}
				if (removedTasks != null) {
					executor.execute(new Runnable() {
						List<String> children;
						OnConsumerCallback cb;
						
						public Runnable init (List<String> children, OnConsumerCallback cb) {
							this.children = children;
							this.cb = cb;
							
							return this;
						}
						
						public void run() {
							if(children == null) {
								return;
							}

							for(String task : children){
								log.info("removed task: {}", task);
								cb.onTaskChanged(task, null, false);  
							}
						}
					}.init(removedTasks, consumerCallback));
				}
				
				break;
			default:
				break;
			}
		}
	};
	
    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                zk.getData(path, false, taskDataCallback, ctx);
                break;
            case OK:
				executor.execute(new Runnable() {
					Object ctx;
					byte[] data;
					
					public Runnable init (Object ctx, byte[] data) {
						this.ctx  = ctx;
						this.data = data;
						
						return this;
					}
					
					public void run() {
		            	byte[] nodeData;
						try {
							nodeData = GzipUtil.ungzip(data);
							consumerCallback.onTaskChanged((String)ctx, nodeData, true);
						} catch (Exception e) {
							e.printStackTrace();
							log.error("taskDataCallback, " + e);
						}
					}
				}.init(ctx, data));
                break;
            default:
                log.error("Failed to get task data, {}, {}", Code.get(rc), path);
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
        if (status.equals(this.workerStatus)) {
        	log.info("updateWorkerStatus, update : {}, status : {}", ZKNodeConst.WORKER_PARENT_NODE + "/" + name, status);
        	
        	byte[] nodeData;
			try {
				nodeData = GzipUtil.gzip(status.getBytes());
				zk.setData(ZKNodeConst.WORKER_PARENT_NODE + "/" + name, nodeData, -1,
						statusUpdateCallback, status);
			} catch (Exception e) {
				e.printStackTrace();
				log.error("updateWorkerStatus, " + e);
			}
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
        log.info("createTaskStatus, task:{}, status:{}", task, status);
        
        taskMap.put(task, status);
        
        byte[] data;
		try {
			data = GzipUtil.gzip(status.getBytes());
			zk.create(ZKNodeConst.STATUS_PARENT_NODE + "/" + task, data, Ids.OPEN_ACL_UNSAFE, 
					CreateMode.PERSISTENT, taskStatusCreateCallback, task);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("createTaskStatus, " + e);
		}
    }
    
    protected ConcurrentHashMap<String, Object> taskMap = new ConcurrentHashMap<String, Object>();
    
    private StringCallback taskStatusCreateCallback = new StringCallback(){
        public void processResult(int rc, String path, Object ctx, String name) {
        	log.info("taskStatusCreateCallback, set task status callback, {}, {}", Code.get(rc), path);
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
            	/*
            	 * 节点消失后，taskManager会重新下发任务
            	 */
                break;
            case OK:
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
        log.info("setTaskStatus, task:{}, status:{}", task, status);
        
        taskMap.put(task, status);
        
        byte[] data;
		try {
			data = GzipUtil.gzip(status.getBytes());
			zk.setData(ZKNodeConst.STATUS_PARENT_NODE + "/" + task, data, -1, taskStatusUpdateCallback, task);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("setTaskStatus, " + e);
		}
    }
    
    private StatCallback taskStatusUpdateCallback = new StatCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			log.info("taskStatusUpdateCallback, {}, {}", Code.get(rc), path);
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
	
    /*
     ************************************************
     ************************************************
     * Methods to update system load of this worker.*
     ************************************************
     ************************************************
     */
	private void updateSysLoad() {
		
		executor.execute(new Runnable() {
			private byte[] data;
			public Runnable init (byte[] data) {
				this.data = data;
				return this;
			}
			
			public void run() {
				while (true) {
					try {
						Thread.sleep(1000 * 60);// 1 minute
						
						WorkerStatusDto workerStatus = new WorkerStatusDto();
						workerStatus.setData(this.data);
						workerStatus.setLoad(getSysLoad());
						workerStatus.setCpuCore(getCpuCore());
						workerStatus.setIp(getIP());
						
						JSONObject workerStatusJson = JSONObject.fromObject(workerStatus);
						
						setWorkerStatus(workerStatusJson.toString());
					} catch (Exception e) {
						log.error("updateClientLoad, {}", e);
					}

				}
			}
		}.init(this.workerData));
	}
	
	private float getSysLoad() {
		Map<String, String> cpuLoadAvarageMap = OSUtils.cpuLoad();
		String load = cpuLoadAvarageMap.get("1Min");
		
		if (load != null) {
			log.info("getSysLoad, " + load);
			float  sysload = Float.valueOf(load);
			return sysload;
		}
		return 0;
	}
	
	private int getCpuCore() {
		int core = OSUtils.cpuCoreNum();
		
		return core;
	}
    
	private String getIP() {
		String ip = "";
		
		try {
			ip = OSUtils.getRealIp();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ip;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
