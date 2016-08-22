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
	
	public void onWorkersChanged(List<String> added, List<String> removed);
	public void onWorkerStatusChanged(String worker, byte[] data);
	
	public void onTaskChanged(Object ctx, byte[] data);
	public void onTaskStatusChanged(String path, Object ctx, byte[] data);
}