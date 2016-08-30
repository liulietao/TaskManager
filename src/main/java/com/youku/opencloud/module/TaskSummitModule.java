/**
 * 
 */
package com.youku.opencloud.module;

import java.io.IOException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.callback.OnProducerCallback;
import com.youku.opencloud.dto.TaskDto;
import com.youku.opencloud.taskmanager.ProducerClient;

/**
 * @author liulietao
 *
 */
public class TaskSummitModule implements OnProducerCallback {

	private static final Logger log = LoggerFactory.getLogger(TaskSummitModule.class);
	
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
	public TaskSummitModule(String zkHost) {
		
		this.zkHost = zkHost;
		
		client = new ProducerClient(zkHost, this);
	}

	public void bootstrap() {
		log.info("bootstrap");
		try {
			client.bootstrap();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		log.info("close");
		client.close();
	}
	
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
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnProducerCallback#onSummitResult(boolean, com.youku.opencloud.dto.TaskDto)
	 */
	@Override
	public void onSummitTaskResult(boolean result, TaskDto task) {
		log.info("on summit task result:{}, summitId:{}", result, task.getSummitId());
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TaskSummitModule module = new TaskSummitModule(args[0]);
		
		module.bootstrap();
		
		for (int i = 0; i < 60; i++) {
			Date date = new Date();
			
			module.summitTasks(Integer.toString(i), date.toString().getBytes());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
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
