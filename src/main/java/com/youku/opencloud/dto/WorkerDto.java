/**
 * 
 */
package com.youku.opencloud.dto;

/**
 * @author liulietao
 *
 */
public class WorkerDto {

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

	private String workerName = "";
	private byte[] data = null;
	
	public WorkerDto() {
		
	}
}
