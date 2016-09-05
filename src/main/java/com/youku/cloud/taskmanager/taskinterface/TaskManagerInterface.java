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
	 * session过期后，禁止调用TaskManagerModule方法，等待onSessionStart回调
	 */
	public void onSessionExpired();
	public void onSessionStart();
	
	/**
	 * @param workerNameList ：新上线worker节点集合，节点名称唯一
	 */
	public void onWorkersAdded(List<String> workerNameList);
	/**
	 * @param workerNameList ：新下线worker节点集合，节点名称唯一
	 */
	public void onWorkersRemoved(List<String> workerNameList);
	/**
	 * @param worker ： worker节点状态变更，包含动态负载和主机配置
	 */
	public void onWorkerStatusChanged(Worker worker);
	
	/**
	 * @param taskName ： 新增任务名称，唯一
	 * @param taskData ： 任务数据
	 */
	public void onTaskChanged(String taskName, byte[] taskData);
	/**
	 * @param taskName ： 任务名称，唯一
	 * @param taskData ： 任务数据
	 * @param statusEnum ：任务 当前状态，START, FINISH, FAILED
	 */
	public void onTaskStatusChanged(String taskName, byte[] taskData, TaskStatusEnum statusEnum);
}
