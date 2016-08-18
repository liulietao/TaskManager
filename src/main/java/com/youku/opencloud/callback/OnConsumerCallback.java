/**
 * 
 */
package com.youku.opencloud.callback;

/**
 * @author liulietao
 *
 */
public interface OnConsumerCallback {
	public void onConnectedFailed();
	public void onConnectedSuccess();
	
	public void onAssignedTask();
}
