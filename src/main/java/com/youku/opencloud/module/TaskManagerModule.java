/**
 * 
 */
package com.youku.opencloud.module;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.callback.OnManagerCallback;
import com.youku.opencloud.dto.TaskDto;
import com.youku.opencloud.dto.TaskStatusDto;
import com.youku.opencloud.dto.WorkerDto;
import com.youku.opencloud.dto.WorkerStatusDto;
import com.youku.opencloud.taskmanager.MasterClient;
import com.youku.opencloud.util.OSUtils;

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
	
	private MasterClient client;
	
	private boolean sessionExpired = false;
	
	/**
	 * 
	 */
	public TaskManagerModule(String zkHost) {
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
	
	protected void flushDB(String taskData) {
		
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onConnectedFailed()
	 */
	@Override
	public void onConnectedFailed() {
		log.info("onConnectedFailed");
		
		sessionExpired = true;
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onConnectedSuccess()
	 */
	@Override
	public void onConnectedSuccess() {
		log.info("onConnectedSuccess");
		
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
		log.info("onWorkerStatusChanged, worker : {}, describe : {}", worker, new String(data));
		
		JSONObject jsonWorker = JSONObject.fromObject(new String(data));
		WorkerStatusDto workerStatusDto = (WorkerStatusDto)JSONObject.toBean(jsonWorker, WorkerStatusDto.class);
		log.info("onWorkerStatusChanged, load 1Min:{}",  workerStatusDto.getLoad());
		
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
		
		flushDB(taskStatus.getData());
		
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
        	
        	WorkerDto worker = null;
        	for(Map.Entry<String, WorkerDto> entry : workerMap.entrySet()) {
        		worker = entry.getValue();
        		break;
        	}
        	
        	TaskDto task = null;
        	for(Map.Entry<String, TaskDto> entry : taskMap.entrySet()) {
        		task = taskMap.remove(entry.getKey());
        		break;
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
				Thread.sleep(1000 * 10);
				
				manager.dumpTasks();
				manager.dumpWorkers();
				
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
