/**
 * 
 */
package com.youku.cloud.taskmanager.dto;

/**
 * @author liulietao
 *
 */
public class WorkerStatusDto {
	
	private String version = "0.0.1";
	private float load = 0;
	private int cpuCore = 1;
	private byte[] data = new byte[0];

	public float getLoad() {
		return load;
	}

	public void setLoad(float load) {
		this.load = load;
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

	public int getCpuCore() {
		return cpuCore;
	}

	public void setCpuCore(int cpuCore) {
		this.cpuCore = cpuCore;
	}

	/**
	 * 
	 */
	public WorkerStatusDto() {
	}
}
