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
	
	/**
	 * 
	 */
	public TaskSummitModule(String zkHost) {
		client = new ProducerClient(zkHost, this);
	}

	public void bootstrap() {
		try {
			client.bootstrap();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void summitTasks() {
		//test
		for (int i = 0; i < 20; i++) {
			TaskDto task = new TaskDto();
			task.setStatus(false);
			task.setTaskName(Integer.toString(i));
			task.setTask(Integer.toString(i));
			
			Date date = new Date();
			
			client.createTask(Integer.toString(i) + "-" + date.toString(), task);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnProducerCallback#onConnectedFailed()
	 */
	@Override
	public void onConnectedFailed() {
		log.error("");
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnProducerCallback#onConnectedSuccess()
	 */
	@Override
	public void onConnectedSuccess() {
		log.debug("");
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnProducerCallback#onSummitResult(boolean, com.youku.opencloud.dto.TaskDto)
	 */
	@Override
	public void onSummitTaskResult(boolean result, TaskDto task) {
		log.debug("on summit task result:{}, task:{}", result, task);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
