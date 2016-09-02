/**
 * 
 */
package com.youku.cloud.taskmanager.taskinterface;

import java.util.List;

/**
 * @author liulietao
 *
 */
public interface TaskManagerInterface {
	/**
	 * session过期后，禁止调用TaskManagerModule方法
	 */
	public void onSessionExpired();
	public void onSessionStart();
	
	public void onWorkersAdded(List<String> workerNameList);
	public void onWorkersRemoved(List<String> workerNameList);
	public void onWorkerStatusChanged(Worker worker);
	
	public void onTaskChanged(String taskName, byte[] taskData);
	public void onTaskStatusChanged(String taskName, byte[] taskData, TaskStatusEnum statusEnum);
}
