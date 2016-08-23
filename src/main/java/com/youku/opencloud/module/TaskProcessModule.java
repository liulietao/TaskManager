/**
 * 
 */
package com.youku.opencloud.module;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.callback.OnConsumerCallback;
import com.youku.opencloud.dto.TaskDto;
import com.youku.opencloud.dto.TaskStatusDto;
import com.youku.opencloud.dto.WorkerStatusDto;
import com.youku.opencloud.taskmanager.ConsumerClient;
import com.youku.opencloud.util.OSUtils;

/**
 * @author liulietao
 *
 */
public class TaskProcessModule implements OnConsumerCallback {

	private static final Logger log = LoggerFactory.getLogger(TaskProcessModule.class);
	
	protected ConsumerClient client;
	
	private ConcurrentHashMap<String, TaskDto> taskMap;
	
	private boolean sessionExpired = false;
	/**
	 * 
	 */
	public TaskProcessModule(String zkHost) {
		client = new ConsumerClient(zkHost, this);
	}
	
	public void bootstrap() {
		log.debug("bootstrap");
		
		try {
			client.bootstrap();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		log.debug("close");
		client.close();
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnConsumerCallback#onConnectedFailed()
	 */
	@Override
	public void onConnectedFailed() {
		log.debug("onConnectedFailed");
		
		sessionExpired = true;
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnConsumerCallback#onConnectedSuccess()
	 */
	@Override
	public void onConnectedSuccess() {
		log.debug("onConnectedSuccess");
		
		sessionExpired = false;
	}

	@Override
	public void onTaskChanged(String task, byte[] data, boolean add) {
		
		if (add == true) {
			TaskDto taskDto = new TaskDto();
			taskDto.setData(data);
			taskDto.setTaskName(task);
			
			taskMap.put(task, taskDto);
		} else {
			stopProcess(task);
		}
	}
	
	public void runProcessRandom() {
		int size = taskMap.size();
		
		if(size > 0) {
			TaskDto taskDto = taskMap.remove(new Random().nextInt(taskMap.size()));
			
			runProcess(taskDto);
		}
	}
	
	protected void runProcess(TaskDto task)	{
		log.debug("run process : {}", task);
		
		TaskStatusDto taskStatus = new TaskStatusDto();
		taskStatus.setStatus(TaskStatusDto.TaskStautsEnum.RUNNING);
		JSONObject taskStatusJson = JSONObject.fromObject(taskStatus);
		
		client.createTaskStatus(task.getTaskName(), taskStatusJson.toString());
		
		for (int i = 0; i < 20; i++) {
			try {
				WorkerStatusDto workerStatus = new WorkerStatusDto();
				workerStatus.setStatus(WorkerStatusDto.WorkerStatusEnum.WORKING);
				workerStatus.setLoad(i);
				JSONObject workerStatusJson = JSONObject.fromObject(workerStatus);
				
				client.setWorkerStatus(workerStatusJson.toString());
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		taskStatus.setStatus(TaskStatusDto.TaskStautsEnum.FINISHED);
		taskStatusJson = JSONObject.fromObject(taskStatus);
		client.setTaskStatus(task.getTaskName(), taskStatusJson.toString());
		
		client.deleteAssignTask(task.getTaskName());
	}
	
	protected void stopProcess(String task) {
		log.debug("stop process : {}", task);
		
		//TODO
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TaskProcessModule module = new TaskProcessModule(args[0]);
		
		module.bootstrap();
		
        while(!module.sessionExpired){
            try {
            	module.runProcessRandom();
            	
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }   
		
		int cpuLoad = OSUtils.cpuUsage();
		log.info("cpu load : {}", cpuLoad);
		
		module.close();
	}
}
