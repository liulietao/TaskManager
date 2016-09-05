/**
 * 
 */
package com.youku.cloud.taskmanager.module;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.cloud.taskmanager.callback.OnConsumerCallback;
import com.youku.cloud.taskmanager.client.ConsumerClient;
import com.youku.cloud.taskmanager.dto.TaskDto;
import com.youku.cloud.taskmanager.dto.TaskStatusDto;
import com.youku.cloud.taskmanager.taskinterface.Task;
import com.youku.cloud.taskmanager.taskinterface.TaskProcessInterface;
import com.youku.cloud.taskmanager.taskinterface.TaskStatusEnum;
import com.youku.cloud.taskmanager.util.OSUtils;

/**
 * @author liulietao
 *
 */
public class TaskProcessModule implements OnConsumerCallback {

	private static final Logger log = LoggerFactory.getLogger(TaskProcessModule.class);
	
	private TaskProcessInterface taskInterface;
	
	private ConsumerClient client;
	
	protected ConcurrentHashMap<String, TaskDto> taskMap = new ConcurrentHashMap<String, TaskDto>();
	protected ConcurrentHashMap<String, TaskDto> taskProcessMap = new ConcurrentHashMap<String, TaskDto>();
	
	private boolean sessionExpired = false;
	
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
	public TaskProcessModule(String zkHost, TaskProcessInterface inter) {
		this.zkHosts = zkHost;
		this.taskInterface = inter;
		
		client = new ConsumerClient(zkHost, this);
	}
	
	/**
	 * 启动TaskProcess模块
	 * @param workerData ： worker节点描述
	 */
	public void bootstrap(byte[] workerData) {
		log.info("bootstrap");
		
		this.workerDescribe = workerData.clone();
		
		try {
			client.bootstrap(workerDescribe);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 停止TaskProcess模块
	 */
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
		taskProcessMap.clear();

		if (taskInterface != null) {
			taskInterface.onSessionExpired();
		}
		
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
		
		if (taskInterface != null) {
			taskInterface.onSessionStart();
		}
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnConsumerCallback#onTaskChanged()
	 */
	@Override
	public void onTaskChanged(String task, byte[] data, boolean add) {
		if (add) {
			TaskDto taskDto = new TaskDto();
			taskDto.setData(data);
			taskDto.setTaskName(task);
			
			taskMap.put(task, taskDto);
		} else {
			taskMap.remove(task);
			taskProcessMap.remove(task);
		}
		
		if (taskInterface != null) {
			taskInterface.onTaskChanged(task, data, add);
		}
	}
	
	/**
	 * 获取任务接口
	 * @return 返回新任务，否则返回null
	 */
	public Task getTask() {
		int size = taskMap.size();
		
		if(size > 0) {
			log.info("getTask");
			
			TaskDto taskDto = null;
			
			for(Map.Entry<String, TaskDto> entry : taskMap.entrySet()) {
				taskDto = taskMap.remove(entry.getKey());
				break;
			}
			
			taskProcessMap.put(taskDto.getTaskName(), taskDto);
			
			Task task = new Task();
			task.setTaskData(taskDto.getData());
			task.setTaskName(taskDto.getTaskName());
			
			return task;
		}
		
		return null;
	}
	
	/**
	 * 更新任务状态接口
	 * @param taskName ：任务名称
	 * @param statusEnum ： 任务状态
	 * @return true,接口调用成功;false,不存在的任务
	 */
	public boolean updateTaskStatus(String taskName, TaskStatusEnum statusEnum) {
		log.info("updateTaskStatus, taskName:{}, status:{}", taskName, statusEnum);
		
		if (taskMap.containsKey(taskName) == false && taskProcessMap.containsKey(taskName) == false) {
//			log.info("taskMap:{}", taskMap);
//			log.info("taskProcessMap:{}", taskProcessMap);
			log.error("updateTaskStatus, not exist task ,taskName:{}", taskName);
			return false;
		}
		
		switch (statusEnum) {
			case START:
				{
					TaskStatusDto taskStatus = new TaskStatusDto();
					taskStatus.setStatus(TaskStatusDto.START);
					JSONObject taskStatusJson = JSONObject.fromObject(taskStatus);
					client.createTaskStatus(taskName, taskStatusJson.toString());
				}
				break;
			case FINISH:
				{
					TaskStatusDto taskStatus = new TaskStatusDto();
					taskStatus.setStatus(TaskStatusDto.FINISHED);
					JSONObject taskStatusJson = JSONObject.fromObject(taskStatus);
					client.setTaskStatus(taskName, taskStatusJson.toString());
					client.deleteAssignTask(taskName);
					taskProcessMap.remove(taskName);
				}
				break;
			case FAILED:
				{
					TaskStatusDto taskStatus = new TaskStatusDto();
					taskStatus.setStatus(TaskStatusDto.FAILED);
					JSONObject taskStatusJson = JSONObject.fromObject(taskStatus);
					client.setTaskStatus(taskName, taskStatusJson.toString());
					client.deleteAssignTask(taskName);
					taskProcessMap.remove(taskName);
				}
				break;
			default:
				log.error("updateTaskStatus, some thing go wrong");
				break;
		}
		return true;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		class ProcessImp implements TaskProcessInterface {
			private final Logger log = LoggerFactory.getLogger(ProcessImp.class);
			@Override
			public void onSessionStart() {
				log.info("onSessionStart");
			}

			@Override
			public void onSessionExpired() {
				log.info("onSessionExpired");
			}

			@Override
			public void onTaskChanged(String taskName, byte[] data, boolean add) {
				log.info("onTaskChanged, taskName:" + taskName + ", add:" + add);
				if (data != null) {
					log.info("onTaskChanged, data:" + new String(data));
				}
			}
		}
		
		ProcessImp processImp = new ProcessImp();
		String zkHosts = args[0];
		TaskProcessModule module = new TaskProcessModule(zkHosts, processImp);
		
		String workerDescribe = "{'name':'split-video','help':'liulietao@youku.com','decribe':'this module is just a tester, so do nothing, just print .'}";
		module.bootstrap(workerDescribe.getBytes());
		
        while(!module.sessionExpired){
            try {
            	Task task = module.getTask();
            	if (task != null) {
            		module.updateTaskStatus(task.getTaskName(), TaskStatusEnum.START);
            		int count = 100;
            		for (int i = 0; i < count; i++) {
            			log.info("process, task:" + task.getTaskName() + ", data:" + new String(task.getTaskData()) + ", at:" + i / (count * 1.0));
            			Thread.sleep(1000);
            		}
            		module.updateTaskStatus(task.getTaskName(), TaskStatusEnum.FINISH);
				}
            	
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }   
		
		int cpuLoad = OSUtils.cpuUsage();
		log.info("quit, cpu load : {}", cpuLoad);
		
		module.close();
	}
}
