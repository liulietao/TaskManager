/**
 * 
 */
package com.youku.cloud.taskmanager.callback;

/**
 * @author liulietao
 *
 */
public interface OnConsumerCallback {
	public void onSessionStart();
	public void onSessionExpired();
	
	public void onTaskChanged(String task, byte[] data, boolean add);
}
