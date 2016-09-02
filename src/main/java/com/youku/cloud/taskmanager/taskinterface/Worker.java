/**
 * 
 */
package com.youku.cloud.taskmanager.taskinterface;

/**
 * @author liulietao
 *
 */
public class Worker {
	
	private String name = "";
	private float loadAverage = 0;
	private int cpuCore = 1;
	private String ip = "";
	private byte[] data = new byte[0];

	/**
	 * 
	 */
	public Worker() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public float getLoadAverage() {
		return loadAverage;
	}

	public void setLoadAverage(float loadAverage) {
		this.loadAverage = loadAverage;
	}

	public int getCpuCore() {
		return cpuCore;
	}

	public void setCpuCore(int cpuCore) {
		this.cpuCore = cpuCore;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
}
