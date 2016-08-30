/**
 * 
 */
package com.youku.cloud.taskmanager.dto;

/**
 * @author liulietao
 *
 */
public class WorkerDto {

	private String version = "0.0.1";
	private String workerName = "";
	private byte[] data = null;
	
	public String getWorkerName() {
		return workerName;
	}

	public void setWorkerName(String workerName) {
		this.workerName = workerName;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public WorkerDto() {
		
	}
}
