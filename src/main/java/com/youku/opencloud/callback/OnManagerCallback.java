/**
 * 
 */
package com.youku.opencloud.callback;

import java.util.List;

/**
 * @author liulietao
 *
 */
public interface OnManagerCallback {
	public void onConnectedFailed();
	public void onConnectedSuccess();
	
	public void onWorkersChanged(List<String> total, List<String> removed);
	public void onWorkerStatusChanged(String path, byte[] data);
	
	public void onTaskChanged(List<String> total);
	public void onTaskData(String path, Object ctx, byte[] data);
	public void onTaskStatusChanged(String path, Object ctx, byte[] data);
}