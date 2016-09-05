/**
 * 
 */
package com.youku.cloud.taskmanager.taskinterface;


/**
 * @author liulietao
 *
 */
public interface TaskSummitInterface {
	/**
	 * session过期后，禁止调用TaskSummitModule方法，等待onSessionStart回调
	 */
	public void onSessionStart();
	public void onSessionExpired();
	
	
	/**
	 * @param result ： 任务提交结果，true：成功，否则失败
	 * @param summitId ： 提交任务的标识，调用者自己维护，不传输
	 */
	public void onSummitTaskResult(boolean result, String summitId);
}
