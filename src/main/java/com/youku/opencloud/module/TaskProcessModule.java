/**
 * 
 */
package com.youku.opencloud.module;

import java.io.IOException;
import java.util.Map;
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
	
	private String zkHosts;
	private String workerDescribe;
	/**
	 * 
	 */
	public TaskProcessModule(String zkHost) {
		this.zkHosts = zkHost;
		
		client = new ConsumerClient(zkHost, this);
		
        this.executor = new ThreadPoolExecutor(8, 8, 
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(100),
                new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	public void bootstrap(String workerDescribe) {
		log.info("bootstrap");
		
		this.workerDescribe = workerDescribe;
		
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
		client = new ConsumerClient(zkHosts, this);
		bootstrap(workerDescribe);
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnConsumerCallback#onConnectedSuccess()
	 */
	@Override
	public void onSessionStart() {
		log.info("onConnectedSuccess");
		
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
			log.info("runProcessRandom");
			
			TaskDto taskDto = null;
			
			for(Map.Entry<String, TaskDto> entry : taskMap.entrySet()) {
				taskDto = taskMap.remove(entry.getKey());
				break;
			}
			
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
				TaskStatusDto taskStatus = new TaskStatusDto();
				taskStatus.setStatus(TaskStatusDto.RUNNING);
				JSONObject taskStatusJson = JSONObject.fromObject(taskStatus);
				
				client.createTaskStatus(task.getTaskName(), taskStatusJson.toString());
				
				for (int i = 0; i < 20; i++) {
					try {
						log.info("runProcess, process task:" + task.getTaskName() + ", data:{}, progress:{}", new String(task.getData()), i/20.0);
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				taskStatus.setStatus(TaskStatusDto.FINISHED);
				taskStatusJson = JSONObject.fromObject(taskStatus);
				client.setTaskStatus(task.getTaskName(), taskStatusJson.toString());
				
				client.deleteAssignTask(task.getTaskName());
			}
		}.init(task));
	}
	
	protected void stopProcess(String task) {
		log.info("stop process : {}", task);
		
		//TODO
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TaskProcessModule module = new TaskProcessModule(args[0]);
		
		module.bootstrap("{'name':'video precess','help':'liulietao@youku.com','decribe':'this module is just a tester, so do nothing, just print .'}");
		
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
