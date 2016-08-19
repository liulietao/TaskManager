/**
 * 
 */
package com.youku.opencloud.callback;

import java.util.List;

/**
 * @author liulietao
 *
 */
public interface OnConsumerCallback {
	public void onConnectedFailed();
	public void onConnectedSuccess();
	
	public void onAssignedTask(List<String> tasks);
}
