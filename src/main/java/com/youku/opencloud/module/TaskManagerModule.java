/**
 * 
 */
package com.youku.opencloud.module;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.callback.OnManagerCallback;
import com.youku.opencloud.taskmanager.ChildrenCache;
import com.youku.opencloud.taskmanager.MasterClient;
import com.youku.opencloud.util.OSUtils;

/**
 * @author liulietao
 *
 */
public class TaskManagerModule implements OnManagerCallback {
	private static final Logger log = LoggerFactory.getLogger(TaskManagerModule.class);
	
	protected ChildrenCache tasksCache;
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
			log.debug("");
			client.bootstrap();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void close() {
		log.debug("");
		client.close();
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onConnectedFailed()
	 */
	@Override
	public void onConnectedFailed() {
		log.debug("");
		
		sessionExpired = true;
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onConnectedSuccess()
	 */
	@Override
	public void onConnectedSuccess() {
		log.debug("");
		
		sessionExpired = false;
		
		client.runForMaster();
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onWorkersChanged(java.util.List, java.util.List)
	 */
	@Override
	public void onWorkersChanged(List<String> total, List<String> removed) {
		log.debug("total : {}, removed : {}", total, removed);
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onWorkerStatusChanged(java.lang.String, byte[])
	 */
	@Override
	public void onWorkerStatusChanged(String path, byte[] data) {
		log.debug("path : {}, data : {}", path, data);
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onTaskChanged(java.util.List)
	 */
	@Override
	public void onTaskChanged(List<String> total) {
		log.debug("total : {}", total);
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onTaskData(java.lang.String, java.lang.Object, byte[])
	 */
	@Override
	public void onTaskData(String path, Object ctx, byte[] data) {
		log.debug("path : {}, data : {}", path, data);
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onTaskStatusChanged(java.lang.String, java.lang.Object, byte[])
	 */
	@Override
	public void onTaskStatusChanged(String path, Object ctx, byte[] data) {
		log.debug("path : {}, data : {}", path, data);
	}
	
	public void assignTask() {
		log.debug("");
//		client.assignTasks(designatedWorker, task, data);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TaskManagerModule manager = new TaskManagerModule(args[0]);
		
		manager.bootstrap();
		
		manager.assignTask();
		
        while(!manager.sessionExpired){
            try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }   
		
		int cpuLoad = OSUtils.cpuUsage();
		log.info("cpu load : {}", cpuLoad);
		
		manager.close();
	}
}
