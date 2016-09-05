/**
 * 
 */
package com.youku.cloud.taskmanager.module;

import java.io.IOException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.cloud.taskmanager.callback.OnProducerCallback;
import com.youku.cloud.taskmanager.client.ProducerClient;
import com.youku.cloud.taskmanager.dto.TaskDto;
import com.youku.cloud.taskmanager.taskinterface.TaskSummitInterface;

/**
 * @author liulietao
 *
 */
public class TaskSummitModule implements OnProducerCallback {

	private static final Logger log = LoggerFactory.getLogger(TaskSummitModule.class);
	
	private TaskSummitInterface taskInterface;
	
	private ProducerClient client;
	
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
	public TaskSummitModule(String zkHost, TaskSummitInterface inter) {
		
		this.zkHost = zkHost;
		this.taskInterface = inter;
		
		client = new ProducerClient(zkHost, this);
	}

	/**
	 * 启动TaskSummit模块
	 */
	public void bootstrap() {
		log.info("bootstrap");
		try {
			client.bootstrap();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 停止TaskSummit模块
	 */
	public void close() {
		log.info("close");
		client.close();
	}
	
	/**
	 * @param summitId ： 提交任务的标识，调用者自己维护，不传输
	 * @param data ： 任务数据
	 */
	public void summitTasks(String summitId, byte[] data) {
		TaskDto task = new TaskDto();
		task.setSummitId(summitId);
		task.setData(data);
		
		log.info("summitTasks, summitId:" + task.getSummitId());
		client.createTask(task);
	}
	
	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnProducerCallback#onConnectedFailed()
	 */
	@Override
	public void onSessionExpired() {
		log.info("onSessionExpired");
		
		sessionExpired = true;
		
		client.close();
		
		if(taskInterface != null) {
			taskInterface.onSessionExpired();
		}
		
		client = new ProducerClient(zkHost, this);
		bootstrap();
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnProducerCallback#onConnectedSuccess()
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
	 * @see com.youku.opencloud.callback.OnProducerCallback#onSummitResult(boolean, com.youku.opencloud.dto.TaskDto)
	 */
	@Override
	public void onSummitTaskResult(boolean result, TaskDto task) {
		log.info("on summit task result:{}, summitId:{}", result, task.getSummitId());
		
		if (taskInterface != null) {
			taskInterface.onSummitTaskResult(result, task.getSummitId());
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		class SummitImp implements TaskSummitInterface {
			private final Logger log = LoggerFactory.getLogger(SummitImp.class);
			@Override
			public void onSessionStart() {
				log.info("onSessionStart");
			}

			@Override
			public void onSessionExpired() {
				log.info("onSessionExpired");
			}

			@Override
			public void onSummitTaskResult(boolean result, String summitId) {
				log.info("onSummitTaskResult, result:{}, summitId:{}", result, summitId);
			}
		}
		
		SummitImp summitImp = new SummitImp();
		
		TaskSummitModule module = new TaskSummitModule(args[0], summitImp);
		
		module.bootstrap();
		
//		try {
//			Thread.sleep(1000 * 60 * 5);
//		} catch (InterruptedException e1) {
//			e1.printStackTrace();
//		}
		
		for (int i = 0; i < 1; i++) {
			Date date = new Date();
			
			module.summitTasks(Integer.toString(i), date.toString().getBytes());
//			try {
//				Thread.sleep(20);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
		}
		
        while(!module.sessionExpired){
            try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
		
		module.close();
	}
}
