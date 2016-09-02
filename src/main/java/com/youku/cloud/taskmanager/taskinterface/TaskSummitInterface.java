/**
 * 
 */
package com.youku.cloud.taskmanager.taskinterface;


/**
 * @author liulietao
 *
 */
public interface TaskSummitInterface {
	public void onSessionStart();
	public void onSessionExpired();
	public void onSummitTaskResult(boolean result, String summitId);
}
