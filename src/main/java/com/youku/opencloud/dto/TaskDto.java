/**
 * 
 */
package com.youku.opencloud.dto;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liulietao
 *
 */
public class TaskDto {

    private String task;
    private String taskName;
    private boolean done = false;
    private boolean succesful = false;
    private CountDownLatch latch = new CountDownLatch(1);
    
    private static final Logger log = LoggerFactory.getLogger(TaskDto.class);
    
	/**
	 * 
	 */
	public TaskDto() {
	}
	
	public String getTask () {
        return task;
    }
    
	public void setTask (String task) {
        this.task = task;
    }
    
	public void setTaskName(String name){
        this.taskName = name;
    }
    
	public String getTaskName (){
        return taskName;
    }
    
	public void setStatus (boolean status){
        succesful = status;
        done = true;
        latch.countDown();
    }
    
	public void waitUntilDone () {
        try{
            latch.await();
        } catch (InterruptedException e) {
            log.warn("InterruptedException while waiting for task to get done");
        }
    }
    
	public synchronized boolean isDone(){
        return done;     
    }
    
	public synchronized boolean isSuccesful(){
        return succesful;
    }

}
