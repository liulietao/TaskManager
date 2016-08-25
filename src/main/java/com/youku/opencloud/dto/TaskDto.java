/**
 * 
 */
package com.youku.opencloud.dto;

/**
 * @author liulietao
 *
 */
public class TaskDto {

	private String version = "0.0.1";
	private String summitId = "";
    private String taskName = "";
    
    private byte[] data = null;
    
	/**
	 * 
	 */
	public TaskDto() {
	}
	
	public TaskDto(TaskDto dto) {
		this.version = new String(dto.getVersion());
		this.summitId = new String(dto.getSummitId());
		this.taskName = new String(dto.getTaskName());
		this.data = dto.getData().clone();
	}

	public String getSummitId() {
		return summitId;
	}

	/**
	 * summit id
	 * @param id
	 */
	public void setSummitId(String id) {
		this.summitId = id;
	}
	
	public void setTaskName (String name){
        this.taskName = name;
    }
    
	public String getTaskName (){
        return taskName;
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
}
