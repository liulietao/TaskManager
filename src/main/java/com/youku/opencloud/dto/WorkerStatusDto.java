/**
 * 
 */
package com.youku.opencloud.dto;

/**
 * @author liulietao
 *
 */
public class WorkerStatusDto {
	private float load = 0;
	private String data = "";

	public float getLoad() {
		return load;
	}

	public void setLoad(float load) {
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
