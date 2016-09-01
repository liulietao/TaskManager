/**
 * 
 */
package com.youku.cloud.taskmanager.module;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.cloud.taskmanager.callback.OnConsumerCallback;
import com.youku.cloud.taskmanager.client.ConsumerClient;
import com.youku.cloud.taskmanager.dto.TaskDto;
import com.youku.cloud.taskmanager.dto.TaskStatusDto;
import com.youku.cloud.taskmanager.util.OSUtils;

/**
 * @author liulietao
 *
 */
public class TaskProcessModule implements OnConsumerCallback {

	private static final Logger log = LoggerFactory.getLogger(TaskProcessModule.class);
	
	private ConsumerClient client;
	
	protected ConcurrentHashMap<String, TaskDto> taskMap = new ConcurrentHashMap<String, TaskDto>();
	
	private boolean sessionExpired = false;
	
	private ThreadPoolExecutor executor;
	
	private String zkHosts;
	private byte[] workerDescribe;

	/**
	 * zkHost : comma separated host:port pairs, each corresponding to a zk server.
	 *  		e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" 
	 *  		If the optional chroot suffix is used the example would look like: 
	 *  		"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a" where the client would be rooted at "/app/a" 
	 *  		and all paths would be relative to this root - 
	 *  		ie getting/setting/etc... "/foo/bar" would result in operations being run on "/app/a/foo/bar" (from the server perspective).
	 */
	public TaskProcessModule(String zkHost) {
		this.zkHosts = zkHost;
		
		client = new ConsumerClient(zkHost, this);
		
		int coreNum = Runtime.getRuntime().availableProcessors();
		
        this.executor = new ThreadPoolExecutor(coreNum * 3, coreNum * 5, 
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	public void bootstrap(byte[] workerData) {
		log.info("bootstrap");
		
		this.workerDescribe = workerData.clone();
		
		try {
			client.bootstrap(workerDescribe);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		log.info("close");
		client.close();
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnConsumerCallback#onConnectedFailed()
	 */
	@Override
	public void onSessionExpired() {
		log.info("onSessionExpired, create new session");
		
		//release resource
		sessionExpired = true;
		taskMap.clear();
		
		//recreate session
		client.close();
		client = new ConsumerClient(zkHosts, this);
		bootstrap(workerDescribe);
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnConsumerCallback#onConnectedSuccess()
	 */
	@Override
	public void onSessionStart() {
		log.info("onSessionStart");
		
		sessionExpired = false;
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnConsumerCallback#onTaskChanged()
	 */
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
	
	/*
	 * 子类需要重写该函数来实现具体逻辑，本函数的实现主要为了测试
	 * 本函数由getTask函数触发
	 */
	protected boolean process(String taskName, byte[] taskData) {
		int count = 3;
		for (int i = 0; i < count; i++) {
			try {
				log.info("process, process task:" + taskName + ", data:{}, progress:{}", taskData, i/(count * 1.0));
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		return true;
	}
	
	public void getTask() {
		int size = taskMap.size();
		
		if(size > 0) {
			log.info("getTask");
			
			TaskDto taskDto = null;
			
			for(Map.Entry<String, TaskDto> entry : taskMap.entrySet()) {
				taskDto = taskMap.remove(entry.getKey());
				break;
			}
			
			runProcess(taskDto);
		}
	}
	
	private void runProcess(TaskDto task)	{
		executor.execute(new Runnable() {
			private TaskDto task;
			public Runnable init (TaskDto task) {
				this.task = task;
				return this;
			}
			
			public void run() {
				TaskStatusDto taskStatus = new TaskStatusDto();
				taskStatus.setStatus(TaskStatusDto.RUNNING);
				JSONObject taskStatusJson = JSONObject.fromObject(taskStatus);
				
				client.createTaskStatus(task.getTaskName(), taskStatusJson.toString());
				
				boolean retCode = process(task.getTaskName(), task.getData());
				if (retCode) {
					taskStatus.setStatus(TaskStatusDto.FINISHED);					
				} else {
					taskStatus.setStatus(TaskStatusDto.FAILED);
				}
				taskStatusJson = JSONObject.fromObject(taskStatus);
				client.setTaskStatus(task.getTaskName(), taskStatusJson.toString());
				
				client.deleteAssignTask(task.getTaskName());
			}
		}.init(task));
	}
	
	/*
	 * 子类需要重写该函数来实现具体逻辑
	 */
	protected void stopProcess(String taskName) {
		log.info("stop process : {}", taskName);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TaskProcessModule module = new TaskProcessModule(args[0]);
		
		String workerDescribe = "{'name':'split-video','help':'liulietao@youku.com','decribe':'this module is just a tester, so do nothing, just print .'}";
		module.bootstrap(workerDescribe.getBytes());
		
        while(!module.sessionExpired){
            try {
            	module.getTask();
            	
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
