/**
 * 
 */
package com.youku.opencloud.dto;

/**
 * @author liulietao
 *
 */
public class WorkerStatusDto {
	
	public static enum WorkerStatusEnum {IDLE, WORKING};
	
	private WorkerStatusEnum status = WorkerStatusEnum.IDLE;
	private int load = 0;
	private String data = "";

	public WorkerStatusEnum getStatus() {
		return status;
	}

	public void setStatus(WorkerStatusEnum status) {
		this.status = status;
	}

	public int getLoad() {
		return load;
	}

	public void setLoad(int load) {
		this.load = load;
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
	public WorkerStatusDto() {
	}
}
