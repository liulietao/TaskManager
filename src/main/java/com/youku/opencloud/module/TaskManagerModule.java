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

/**
 * @author liulietao
 *
 */
public class TaskManagerModule implements OnManagerCallback {
	private static final Logger log = LoggerFactory.getLogger(TaskManagerModule.class);
	
	protected ChildrenCache tasksCache;
	private MasterClient client;
	
	/**
	 * 
	 */
	public TaskManagerModule(String zkHost) {
		client = new MasterClient(zkHost, this);
	}
	
	public void bootstrap() {
		try {
			client.bootstrap();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onConnectedFailed()
	 */
	@Override
	public void onConnectedFailed() {
		log.debug("");
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.Callback.OnManagerCallback#onConnectedSuccess()
	 */
	@Override
	public void onConnectedSuccess() {
		log.debug("");
		
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}
}
