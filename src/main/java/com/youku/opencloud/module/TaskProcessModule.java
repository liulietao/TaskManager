/**
 * 
 */
package com.youku.opencloud.module;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.callback.OnConsumerCallback;
import com.youku.opencloud.taskmanager.ChildrenCache;
import com.youku.opencloud.taskmanager.ConsumerClient;
import com.youku.opencloud.util.OSUtils;

/**
 * @author liulietao
 *
 */
public class TaskProcessModule implements OnConsumerCallback {

	private static final Logger log = LoggerFactory.getLogger(TaskProcessModule.class);
	
	protected ConsumerClient client;
	
	protected ChildrenCache tasksCache;
	protected ChildrenCache tasksWatcher;
	
	private boolean sessionExpired = false;
	/**
	 * 
	 */
	public TaskProcessModule(String zkHost) {
		client = new ConsumerClient(zkHost, this);
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

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnConsumerCallback#onConnectedFailed()
	 */
	@Override
	public void onConnectedFailed() {
		log.debug("onConnectedFailed");
		
		sessionExpired = true;
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnConsumerCallback#onConnectedSuccess()
	 */
	@Override
	public void onConnectedSuccess() {
		log.debug("onConnectedSuccess");
		
		sessionExpired = false;
	}

	/* (non-Javadoc)
	 * @see com.youku.opencloud.callback.OnConsumerCallback#onAssignedTask(java.util.List)
	 */
	@Override
	public void onAssignedTask(List<String> children) {
		log.debug("onAssignedTask, assigned task : {}", children);
		
		List<String> newTask;
		if (tasksWatcher == null) {
			tasksWatcher = new ChildrenCache(children);
			newTask = children;
		} else {
			newTask = tasksWatcher.addedAndSet(children);
		}
		
		List<String> removedTask;
		if (tasksCache == null) {
			tasksCache = new ChildrenCache(children);
			removedTask = null;
		} else {
			removedTask = tasksCache.removedAndSet(children);
		}
		
		if (newTask != null) {
			for(String nt : newTask) {
				runProcess(nt);				
			}
		}
		
		if (removedTask != null) {
			for(String rt : removedTask) {
				stopProcess(rt);
			}
		}			
	}
	
	protected void runProcess(String task)	{
		log.debug("run process : {}", task);
		
		client.createTaskStatus(task, "run");
		for (int i = 0; i < 20; i++) {
			try {
				client.setWorkerStatus("{status:run, load:50}");
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		client.setTaskStatus(task, "finish");
	}
	
	protected void stopProcess(String task) {
		log.debug("stop process : {}", task);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TaskProcessModule module = new TaskProcessModule(args[0]);
		
		module.bootstrap();
		
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
