/**
 * 
 */
package com.youku.opencloud.dto;

/**
 * @author liulietao
 *
 */
public class TaskDto {

	private String summitId = "";
    private String taskName = "";
    
    private byte[] data = null;
    
	/**
	 * 
	 */
	public TaskDto() {
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
}
