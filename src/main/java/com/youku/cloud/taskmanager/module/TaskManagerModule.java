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
import com.youku.cloud.taskmanager.util.OSUtils;

/**
 * @author liulietao
 *
 */
public class TaskManagerModule implements OnManagerCallback {
	private static final Logger log = LoggerFactory.getLogger(TaskManagerModule.class);
	
	protected ConcurrentHashMap<String, TaskDto> taskMap = new ConcurrentHashMap<String, TaskDto>();
	protected ConcurrentHashMap<String, TaskDto> taskProcessMap = new ConcurrentHashMap<String, TaskDto>();
	protected ConcurrentHashMap<String, TaskDto> taskFailedMap = new ConcurrentHashMap<String, TaskDto>();
	
	protected ConcurrentHashMap<String, WorkerDto> workerMap = new ConcurrentHashMap<String, WorkerDto>();
	
	private Random random = new Random(this.hashCode());
	
	protected MasterClient client;
	
	private boolean sessionExpired = false;
	
	private String zkHost;
	
	/**
	 * zkHost : comma separated host:port pairs, each corresponding to a zk server.
	 *  		e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" 
	 *  		If the optional chroot suffix is used the example would look like: 
	 *  		"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a" where the client would be rooted at "/app/a" 
	 *  		and all paths would be relative to this root - 
	 *  		ie getting/setting/etc... "/foo/bar" would result in operations being run on "/app/a/foo/bar" (from the server perspective).
	 */
	public TaskManagerModule(String zkHost) {
		this.zkHost = zkHost;
		client = new MasterClient(zkHost, this);
	}
	
	public void bootstrap() {
		try {
			log.info("bootstrap");
			client.bootstrap();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
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
		sessionExpired = true;
		taskMap.clear();
		taskProcessMap.clear();
		taskFailedMap.clear();
		workerMap.clear();
		
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
		
		sessionExpired = false;
		
		client.runForMaster();
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
		}
		
		if (removed != null) {
			for (String w : removed) {
				workerMap.remove(w);
			}			
		}
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onWorkerStatusChanged(java.lang.String, byte[])
	 */
	@Override
	public void onWorkerStatusChanged(String worker, byte[] data) {
		JSONObject jsonWorker = JSONObject.fromObject(new String(data));
		WorkerStatusDto workerStatusDto = (WorkerStatusDto)JSONObject.toBean(jsonWorker, WorkerStatusDto.class);
		log.info("onWorkerStatusChanged, cpuCores:" + workerStatusDto.getCpuCore() + ", load average:" + workerStatusDto.getLoad() 
				+ ", worker data:" + new String(workerStatusDto.getData()));
		
		WorkerDto workerCache = workerMap.get(worker);
		if (workerCache == null) {
			workerCache = new WorkerDto();
		}
		
		workerCache.setData(data);
		workerCache.setWorkerName(worker);
		workerMap.put(worker, workerCache);
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
			taskFailedMap.put(taskName, taskDto);
		}
		
		if (taskStatus.getStatus().equals(TaskStatusDto.FINISHED) || 
				taskStatus.getStatus().equals(TaskStatusDto.FAILED)) {
			client.deleteTaskStatus(taskName);			
		}
	}
	
    /*
     * Choose worker at random.
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
	
	public void dumpTasks() {
		if (taskMap.size() > 0) {
			log.info("dumpTasks:{} \n{}\n", taskMap.size(), taskMap);			
		}
		if (taskFailedMap.size() > 0) {
			log.info("dump failed tasks:{} \n{}\n", taskFailedMap.size(), taskFailedMap);			
		}
	}
	
	public void dumpWorkers() {
		if (workerMap.size() > 0) {
			log.info("dumpWorkers:{} \n{}\n", workerMap.size(), workerMap);			
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TaskManagerModule manager = new TaskManagerModule(args[0]);
		
		manager.bootstrap();
		
        while(!manager.sessionExpired){
            try {
				Thread.sleep(1000 * 1);
				
//				manager.dumpTasks();
//				manager.dumpWorkers();
				
				manager.assignTaskRandom();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }   
		
		int cpuLoad = OSUtils.cpuUsage();
		log.info("cpu load : {}", cpuLoad);
		
		manager.close();
	}
}
