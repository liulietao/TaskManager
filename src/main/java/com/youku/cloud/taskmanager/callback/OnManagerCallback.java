/**
 * 
 */
package com.youku.cloud.taskmanager.callback;

import java.util.List;

/**
 * @author liulietao
 *
 */
public interface OnManagerCallback {
	public void onSessionExpired();
	public void onSessionStart();
	
	public void onWorkersChanged(List<String> added, List<String> removed);
	public void onWorkerStatusChanged(String worker, byte[] data);
	
	public void onTaskChanged(String taskName, byte[] data);
	public void onTaskStatusChanged(String taskName, byte[] data);
}