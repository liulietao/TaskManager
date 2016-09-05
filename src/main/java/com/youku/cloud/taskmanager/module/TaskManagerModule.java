/**
 * 
 */
package com.youku.cloud.taskmanager.module;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.cloud.taskmanager.callback.OnManagerCallback;
import com.youku.cloud.taskmanager.client.MasterClient;
import com.youku.cloud.taskmanager.dto.TaskDto;
import com.youku.cloud.taskmanager.dto.TaskStatusDto;
import com.youku.cloud.taskmanager.dto.WorkerDto;
import com.youku.cloud.taskmanager.dto.WorkerStatusDto;
import com.youku.cloud.taskmanager.taskinterface.TaskManagerInterface;
import com.youku.cloud.taskmanager.taskinterface.TaskStatusEnum;
import com.youku.cloud.taskmanager.taskinterface.Worker;

/**
 * @author liulietao
 *
 */
public class TaskManagerModule implements OnManagerCallback {
	private static final Logger log = LoggerFactory.getLogger(TaskManagerModule.class);
	
	private TaskManagerInterface taskInterface;
	
	protected ConcurrentHashMap<String, TaskDto> taskMap = new ConcurrentHashMap<String, TaskDto>();
	protected ConcurrentHashMap<String, TaskDto> taskProcessMap = new ConcurrentHashMap<String, TaskDto>();
	protected ConcurrentHashMap<String, TaskDto> taskFailedMap = new ConcurrentHashMap<String, TaskDto>();
	
	protected ConcurrentHashMap<String, WorkerDto> workerMap = new ConcurrentHashMap<String, WorkerDto>();
	
	private Random random = new Random(this.hashCode());
	
	protected MasterClient client;
	
	private String zkHost;
	
	/**
	 * zkHost : comma separated host:port pairs, each corresponding to a zk server.
	 *  		e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" 
	 *  		If the optional chroot suffix is used the example would look like: 
	 *  		"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a" where the client would be rooted at "/app/a" 
	 *  		and all paths would be relative to this root - 
	 *  		ie getting/setting/etc... "/foo/bar" would result in operations being run on "/app/a/foo/bar" (from the server perspective).
	 */
	public TaskManagerModule(String zkHost, TaskManagerInterface inter) {
		this.zkHost = zkHost;
		this.taskInterface = inter;
		
		client = new MasterClient(zkHost, this);
	}
	
	/**
	 * 启动TaskManager模块
	 */
	public void bootstrap() {
		try {
			log.info("bootstrap");
			client.bootstrap();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 停止TaskManager模块
	 */
	public void close() {
		log.info("close");
		client.close();
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onConnectedFailed()
	 */
	@Override
	public void onSessionExpired() {
		log.info("onSessionExpired");
		
		//release resource
		taskMap.clear();
		taskProcessMap.clear();
		taskFailedMap.clear();
		workerMap.clear();
		
		if (taskInterface != null) {
			taskInterface.onSessionExpired();
		}
		
		//recreate session
		client.close();
		client = new MasterClient(zkHost, this);
		bootstrap();
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onConnectedSuccess()
	 */
	@Override
	public void onSessionStart() {
		log.info("onSessionStart");
		
		client.runForMaster();
		
		if (taskInterface != null) {
			taskInterface.onSessionStart();
		}
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onWorkersChanged(java.util.List, java.util.List)
	 */
	@Override
	public void onWorkersChanged(List<String> added, List<String> removed) {
		log.info("onWorkersChanged, added : {}, removed : {}", added, removed);
		
		if (added != null) {
			for(String w : added) {
				WorkerDto worker = new WorkerDto();
				worker.setWorkerName(w);
				workerMap.put(w, worker);
			}
			if (taskInterface != null) {
				taskInterface.onWorkersAdded(added);
			}
		}
		
		if (removed != null) {
			for (String w : removed) {
				workerMap.remove(w);
			}
			if (taskInterface != null) {
				taskInterface.onWorkersRemoved(removed);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onWorkerStatusChanged(java.lang.String, byte[])
	 */
	@Override
	public void onWorkerStatusChanged(String workerName, byte[] data) {
		JSONObject jsonWorker = JSONObject.fromObject(new String(data));
		WorkerStatusDto workerStatusDto = (WorkerStatusDto)JSONObject.toBean(jsonWorker, WorkerStatusDto.class);
		log.info("onWorkerStatusChanged, cpuCores:" + workerStatusDto.getCpuCore() + ", load average:" + workerStatusDto.getLoad() 
				+ ", ip:" + workerStatusDto.getIp() + ", worker data:" + new String(workerStatusDto.getData()));
		
		WorkerDto workerCache = workerMap.get(workerName);
		if (workerCache == null) {
			workerCache = new WorkerDto();
		}
		
		workerCache.setData(data);
		workerCache.setWorkerName(workerName);
		workerMap.put(workerName, workerCache);
		
		if (taskInterface != null) {
			Worker worker = new Worker();
			worker.setName(workerName);
			worker.setLoadAverage(workerStatusDto.getLoad());
			worker.setIp(workerStatusDto.getIp());
			worker.setData(workerStatusDto.getData());
			worker.setCpuCore(workerStatusDto.getCpuCore());
			taskInterface.onWorkerStatusChanged(worker);
		}
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onTaskData(java.lang.String, java.lang.Object, byte[])
	 */
	@Override
	public void onTaskChanged(String taskName, byte[] data) {
		log.info("onTaskChanged, task : {}, data : {}", taskName, new String(data));
		
		TaskDto taskDto = new TaskDto();
		taskDto.setData(data);
		taskDto.setTaskName(taskName);
		
		taskMap.put(taskName, taskDto);
		
		if (taskInterface != null) {
			taskInterface.onTaskChanged(taskName, data);
		}
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onTaskStatusChanged(java.lang.String, java.lang.Object, byte[])
	 */
	@Override
	public void onTaskStatusChanged(String taskName, byte[] data) {
		log.info("onTaskStatusChanged, task : {}, data : {}", taskName, new String(data));
		
		JSONObject jsonTask = JSONObject.fromObject(new String(data));
		TaskStatusDto taskStatus = (TaskStatusDto) JSONObject.toBean(jsonTask, TaskStatusDto.class);
		
		if (taskStatus.getStatus().equals(TaskStatusDto.FAILED)) {
			TaskDto taskDto = taskProcessMap.remove(taskName);
			if (taskDto != null) {
				taskFailedMap.put(taskName, taskDto);				
			}
		}
		
		if (taskStatus.getStatus().equals(TaskStatusDto.FINISHED) || 
				taskStatus.getStatus().equals(TaskStatusDto.FAILED)) {
			client.deleteTaskStatus(taskName);	
		}
		
		TaskStatusEnum statusEnum = TaskStatusEnum.FINISH;
		if(taskStatus.getStatus().equals(TaskStatusDto.FAILED)) {
			statusEnum = TaskStatusEnum.FAILED;
		}else if (taskStatus.getStatus().equals(TaskStatusDto.START)) {
			statusEnum = TaskStatusEnum.START;
		}else if (taskStatus.getStatus().equals(TaskStatusDto.FINISHED)) {
			statusEnum = TaskStatusEnum.FINISH;
		}
		
		if (taskInterface != null) {
			taskInterface.onTaskStatusChanged(taskName, data, statusEnum);
		}
	}
	
	/**
	 * 分配任务到随机worker节点
	 */
	public void assignTaskRandom() {
        int workerSize = workerMap.size();
        int taskSize   = taskMap.size();
        
        if (workerSize > 0 && taskSize > 0) {
        	log.info("assignTaskRandom");
        	
        	int randomIndex = random.nextInt(workerSize);
        	WorkerDto worker = null;
        	for(Map.Entry<String, WorkerDto> entry : workerMap.entrySet()) {
        		worker = entry.getValue();
        		
        		if (randomIndex-- <= 0) {
        			break;
				}
        	}
        	
        	TaskDto task = null;
        	if (taskFailedMap.size() > 0) {
	        	for(Map.Entry<String, TaskDto> entry : taskFailedMap.entrySet()) {
	        		task = taskFailedMap.remove(entry.getKey());
	        		break;
	        	}
			} else {
	        	for(Map.Entry<String, TaskDto> entry : taskMap.entrySet()) {
	        		task = taskMap.remove(entry.getKey());
	        		break;
	        	}
			}
        	
        	taskProcessMap.put(task.getTaskName(), task);
        	
        	client.assignTasks(worker.getWorkerName(), task.getTaskName(), task.getData());
        }
	}
	
	/**
	 * 分配任务接口
	 * @param workerName ： worker节点名称，由TaskManagerInterface接口返回
	 * @param taskName ： 任务名称，由TaskManagerInterface接口返回
	 * @param taskData ： 任务数据
	 * @return true：接口调用成功，false：不存在的worker或task
	 */
	public boolean assignTask(String workerName, String taskName, byte[] taskData) {
		if (workerMap.containsKey(workerName) == false) {
			log.error("assignTask, not exist worker:" + workerName);
			return false;
		}
		
		if (taskMap.containsKey(taskName) == false) {
			log.error("assignTask, not exist task:" + taskName);
			return false;
		}
    	
    	client.assignTasks(workerName, taskName, taskData);
		return true;
	}
	
	/**
	 * 打印当前未分配任务和失败待分配任务
	 */
	public void dumpTasks() {
		if (taskMap.size() > 0) {
			log.info("dumpTasks:{} \n{}\n", taskMap.size(), taskMap);			
		}
		if (taskFailedMap.size() > 0) {
			log.info("dump failed tasks:{} \n{}\n", taskFailedMap.size(), taskFailedMap);			
		}
	}
	
	/**
	 * 打印当前在线worker节点
	 */
	public void dumpWorkers() {
		if (workerMap.size() > 0) {
			log.info("dumpWorkers:{} \n{}\n", workerMap.size(), workerMap);			
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		class ManagerImp implements TaskManagerInterface {
			private final Logger log = LoggerFactory.getLogger(ManagerImp.class);
			@Override
			public void onSessionExpired() {
				log.info("onSessionExpired");
			}

			@Override
			public void onSessionStart() {
				log.info("onSessionStart");
			}

			@Override
			public void onWorkersAdded(List<String> workerNameList) {
				log.info("onWorkersAdded, {}", workerNameList);
			}

			@Override
			public void onWorkersRemoved(List<String> workerNameList) {
				log.info("onWorkersRemoved, {}", workerNameList);
			}

			@Override
			public void onWorkerStatusChanged(Worker worker) {
				log.info("onWorkerStatusChanged, workerName:" + worker.getName() + ", load:"
						+ worker.getLoadAverage() + ", cpuCore:" + worker.getCpuCore() 
						+ ", ip:" + worker.getIp() + ", data:" + new String(worker.getData()));
			}

			@Override
			public void onTaskChanged(String taskName, byte[] taskData) {
				log.info("onTaskChanged, taskName:{}, taskData:{}", taskName, taskData);
			}

			@Override
			public void onTaskStatusChanged(String taskName, byte[] taskData, TaskStatusEnum statusEnum) {
				log.info("onTaskStatusChanged, taskName:{}, {}", taskName, statusEnum);
			}
		}
		
		ManagerImp managerImp = new ManagerImp();
		TaskManagerModule manager = new TaskManagerModule(args[0], managerImp);
		
		manager.bootstrap();
		
        while(true){
            try {
				Thread.sleep(200 * 1);
				
//				manager.dumpTasks();
//				manager.dumpWorkers();
				
				manager.assignTaskRandom();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }   
		
//		log.info("quit, cpu load : {}");
//		
//		manager.close();
	}
}
