/**
 * 
 */
package com.youku.opencloud.module;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.callback.OnConsumerCallback;
import com.youku.opencloud.dto.TaskDto;
import com.youku.opencloud.dto.TaskStatusDto;
import com.youku.opencloud.taskmanager.ConsumerClient;
import com.youku.opencloud.util.OSUtils;

/**
 * @author liulietao
 *
 */
public class TaskProcessModule implements OnConsumerCallback {

	private static final Logger log = LoggerFactory.getLogger(TaskProcessModule.class);
	
	protected ConsumerClient client;
	
	private ConcurrentHashMap<String, TaskDto> taskMap = new ConcurrentHashMap<String, TaskDto>();
	
	private boolean sessionExpired = false;
	
	private ThreadPoolExecutor executor;
	/**
	 * 
	 */
	public TaskProcessModule(String zkHost) {
		client = new ConsumerClient(zkHost, this);
		
        this.executor = new ThreadPoolExecutor(1, 1, 
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(100));
	}
	
	public void bootstrap(String workerDescribe) {
		log.debug("bootstrap");
		
		try {
			client.bootstrap(workerDescribe);
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
		
		taskMap.clear();
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
		
		executor.execute(new Runnable() {
			private TaskDto task;
			public Runnable init (TaskDto task) {
				this.task = task;
				return this;
			}
			
			public void run() {
				log.debug("run process : {}", task);
				
				TaskStatusDto taskStatus = new TaskStatusDto();
				taskStatus.setStatus(TaskStatusDto.TaskStautsEnum.RUNNING);
				JSONObject taskStatusJson = JSONObject.fromObject(taskStatus);
				
				client.createTaskStatus(task.getTaskName(), taskStatusJson.toString());
				
				for (int i = 0; i < 20; i++) {
					try {
						Thread.sleep(1000);
						log.debug("process task:{}", task);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				taskStatus.setStatus(TaskStatusDto.TaskStautsEnum.FINISHED);
				taskStatusJson = JSONObject.fromObject(taskStatus);
				client.setTaskStatus(task.getTaskName(), taskStatusJson.toString());
				
				client.deleteAssignTask(task.getTaskName());
			}
		}.init(task));
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
		
		module.bootstrap("{'name':'video precess','help':'liulietao@youku.com'}");
		
        while(!module.sessionExpired){
            try {
            	module.runProcessRandom();
            	
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }   
		
		int cpuLoad = OSUtils.cpuUsage();
		log.info("cpu load : {}", cpuLoad);
		
		module.close();
	}
}
