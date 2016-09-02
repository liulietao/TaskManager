/**
 * 
 */
package com.youku.cloud.taskmanager.taskinterface;

/**
 * @author liulietao
 *
 */
public class Task {

	private String taskName;
	private byte[] taskData;
	
	/**
	 * 
	 */
	public Task() {
		// TODO Auto-generated constructor stub
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public byte[] getTaskData() {
		return taskData;
	}

	public void setTaskData(byte[] taskData) {
		this.taskData = taskData;
	}

}
