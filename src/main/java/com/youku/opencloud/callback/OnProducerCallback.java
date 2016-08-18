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
	public void onConnectedFailed();
	public void onConnectedSuccess();
	
	public void onSummitTaskResult(boolean result, TaskDto task);
}
