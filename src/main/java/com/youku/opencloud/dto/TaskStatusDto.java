/**
 * 
 */
package com.youku.opencloud.dto;

/**
 * @author liulietao
 *
 */
public class TaskStatusDto {

	public static enum TaskStautsEnum {RUNNING, FINISHED, FAILED};
	
	private TaskStautsEnum status;
	private String data = "";
	
	public TaskStautsEnum getStatus() {
		return status;
	}

	public void setStatus(TaskStautsEnum status) {
		this.status = status;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	/**
	 * 
	 */
	public TaskStatusDto() {
	}
}
