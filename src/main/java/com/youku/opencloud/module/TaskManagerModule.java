/**
 * 
 */
package com.youku.opencloud.module;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.callback.OnManagerCallback;
import com.youku.opencloud.dto.TaskDto;
import com.youku.opencloud.dto.TaskStatusDto;
import com.youku.opencloud.dto.TaskStatusDto.TaskStautsEnum;
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
	
	private Random random = new Random(this.hashCode());
	
	protected ConcurrentHashMap<String, TaskDto> taskMap;
	protected ConcurrentHashMap<String, TaskDto> taskProcessMap;
	protected ConcurrentHashMap<String, TaskDto> taskFailedMap;
	
	protected ConcurrentHashMap<String, WorkerDto> workerMap;
	
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
			log.debug("bootstrap");
			client.bootstrap();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		log.debug("close");
		client.close();
	}
	
	protected void flushDB(String taskData) {
		
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onConnectedFailed()
	 */
	@Override
	public void onConnectedFailed() {
		log.debug("onConnectedFailed");
		
		sessionExpired = true;
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onConnectedSuccess()
	 */
	@Override
	public void onConnectedSuccess() {
		log.debug("onConnectedSuccess");
		
		sessionExpired = false;
		
		client.runForMaster();
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onWorkersChanged(java.util.List, java.util.List)
	 */
	@Override
	public void onWorkersChanged(List<String> added, List<String> removed) {
		log.debug("onWorkersChanged, added : {}, removed : {}", added, removed);
		
		for(String w : added) {
			WorkerDto worker = new WorkerDto();
			worker.setWorkerName(w);
			workerMap.put(w, worker);
		}
		
		for (String w : removed) {
			workerMap.remove(w);
		}
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onWorkerStatusChanged(java.lang.String, byte[])
	 */
	@Override
	public void onWorkerStatusChanged(String worker, byte[] data) {
		log.debug("onWorkerStatusChanged, worker : {}, data : {}", worker, new String(data));
		
		JSONObject jsonWorker = JSONObject.fromObject(new String(data));
		WorkerStatusDto workerStatusDto = (WorkerStatusDto)JSONObject.toBean(jsonWorker, WorkerStatusDto.class);
		log.info("onWorkerStatusChanged, status:{}, load:{}", workerStatusDto.getStatus(), workerStatusDto.getLoad());
		
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
		log.debug("onTaskData, task : {}, data : {}", taskName, new String(data));
		
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
		log.debug("onTaskStatusChanged, task : {}, data : {}", taskName, new String(data));
		
		JSONObject jsonTask = JSONObject.fromObject(new String(data));
		TaskStatusDto taskStatus = (TaskStatusDto) JSONObject.toBean(jsonTask, TaskStatusDto.class);
		
		log.info("onTaskStatusChanged, {}", taskStatus.getStatus());
		
		if (taskStatus.getStatus() == TaskStautsEnum.FAILED) {
			TaskDto taskDto = taskProcessMap.remove(taskName);
			taskFailedMap.put(taskName, taskDto);
		}
		
		flushDB(taskStatus.getData());
		
		client.deleteTaskStatus(taskName);
	}
	
    /*
     * Choose worker at random.
     */
	public void assignTaskRandom() {
		log.debug("assignTaskRandom");

        int workerSize = workerMap.size();
        int taskSize   = taskMap.size();
        
        if (workerSize > 0 && taskSize > 0) {
        	WorkerDto worker = workerMap.get(random.nextInt(workerSize));
        	TaskDto task = taskMap.remove(random.nextInt(taskSize));
        	
        	taskProcessMap.put(task.getTaskName(), task);
        	
        	client.assignTasks(worker.getWorkerName(), task.getTaskName(), task.getData());
        }
	}
	
	public void dumpTasks() {
		log.info("dumpTasks:{} \n{}\n", taskMap.size(), taskMap);
		log.info("dump failed tasks:{} \n{}\n", taskFailedMap.size(), taskFailedMap);
	}
	
	public void dumpWorkers() {
		log.info("dumpWorkers \n{}\n", workerMap);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TaskManagerModule manager = new TaskManagerModule(args[0]);
		
		manager.bootstrap();
		
		manager.assignTaskRandom();
		
        while(!manager.sessionExpired){
            try {
				Thread.sleep(3000);
				
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
