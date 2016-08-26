/**
 * 
 */
package com.youku.opencloud.callback;

import com.youku.opencloud.dto.TaskDto;

/**
 * @author liulietao
 *
 */
public interface OnProducerCallback {
	public void onSessionExpired();
	public void onSessionStart();
	
	public void onSummitTaskResult(boolean result, TaskDto task);
}
