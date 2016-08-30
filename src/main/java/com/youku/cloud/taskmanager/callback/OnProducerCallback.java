/**
 * 
 */
package com.youku.cloud.taskmanager.callback;

import com.youku.cloud.taskmanager.dto.TaskDto;

/**
 * @author liulietao
 *
 */
public interface OnProducerCallback {
	public void onSessionExpired();
	public void onSessionStart();
	
	public void onSummitTaskResult(boolean result, TaskDto task);
}
