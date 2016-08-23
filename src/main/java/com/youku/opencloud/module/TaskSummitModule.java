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
import com.youku.opencloud.util.OSUtils;

/**
 * @author liulietao
 *
 */
public class TaskSummitModule implements OnProducerCallback {

	private static final Logger log = LoggerFactory.getLogger(TaskSummitModule.class);
	
	private ProducerClient client;
	
	private boolean sessionExpired = false;
	
	/**
	 * 
	 */
	public TaskSummitModule(String zkHost) {
		client = new ProducerClient(zkHost, this);
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
	
	public void summitTasks(String summitId, byte[] data) {
		TaskDto task = new TaskDto();
		task.setSummitId(summitId);
		task.setData(data);
		
		log.debug("summitTasks, " + task.getSummitId());
		client.createTask(task);
	}
	
	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnProducerCallback#onConnectedFailed()
	 */
	@Override
	public void onConnectedFailed() {
		log.error("onConnectedFailed");
		
		sessionExpired = true;
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnProducerCallback#onConnectedSuccess()
	 */
	@Override
	public void onConnectedSuccess() {
		log.debug("onConnectedSuccess");
		
		sessionExpired = false;
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnProducerCallback#onSummitResult(boolean, com.youku.opencloud.dto.TaskDto)
	 */
	@Override
	public void onSummitTaskResult(boolean result, TaskDto task) {
		log.debug("on summit task result:{}, taskId:{}", result, task.getSummitId());
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TaskSummitModule module = new TaskSummitModule(args[0]);
		
		module.bootstrap();
		
		for (int i = 0; i < 2; i++) {
			Date date = new Date();
			
			module.summitTasks(Integer.toString(i), date.toString().getBytes());
		}
		
        while(!module.sessionExpired){
            try {
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
