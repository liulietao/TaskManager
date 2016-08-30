/**
 * 
 */
package com.youku.cloud.taskmanager.dto;

/**
 * @author liulietao
 *
 */
public class TaskStatusDto {

	public static enum TaskStautsEnum {RUNNING, FINISHED, FAILED};
	
    /* RUNNING */
    public final static String RUNNING  = "RUNNING";

    /* FINISHED */
    public final static String FINISHED = "FINISHED";

    /* FAILED */
    public final static String FAILED 	= "FAILED";
	
	private String version = "0.0.1";
	private String status = "";
	private String data = "";
	
	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	/**
	 * 
	 */
	public TaskStatusDto() {
	}
}
