/**
 * 
 */
package com.youku.cloud.taskmanager.taskinterface;

/**
 * @author liulietao
 *
 */
public interface TaskProcessInterface {
	/*
	 * 
	 */
	public void onSessionStart();
	
	/**
	 * session过期后，应该停止所有正在进行和将要进行的task，并清空所有task缓存，不更新task状态
	 */
	public void onSessionExpired();
	
	/**
	 * @param taskName : 任务节点标识
	 * @param data : 任务数据
	 * @param add : true为新增任务，false为取消任务
	 */
	public void onTaskChanged(String taskName, byte[] data, boolean add);
}
