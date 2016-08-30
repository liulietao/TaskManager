package com.youku.cloud.taskmanager.client;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.cloud.taskmanager.constant.ZKNodeConst;
import com.youku.cloud.taskmanager.util.GzipUtil;

public class TaskRecover {
    private static final Logger LOG = LoggerFactory.getLogger(TaskRecover.class);
    
    /*
     * Various lists wew need to keep track of.
     */
    List<String> tasks;
    List<String> assignments;
    List<String> status;
    List<String> activeWorkers;
    List<String> assignedWorkers;
    
    RecoveryCallback cb;
    
    ZooKeeper zk;
    
    /**
     * Callback interface. Called once 
     * recovery completes or fails.
     *
     */
    public interface RecoveryCallback {
        final static int OK = 0;
        final static int FAILED = -1;
        
        public void recoveryComplete(int rc, List<String> tasks);
    }
    
    /**
     * Recover unassigned tasks.
     * 
     * @param zk
     */
	public TaskRecover(ZooKeeper zk) {
        this.zk = zk;
        this.assignments = new ArrayList<String>();
	}

	/**
     * Starts recovery.
     * 
     * @param recoveryCallback
     */
    public void recover(RecoveryCallback recoveryCallback){
        // Read task list with getChildren
        cb = recoveryCallback;
        getTasks();
    }
    
    private void getTasks(){
        zk.getChildren(ZKNodeConst.TASK_PARENT_NODE, false, tasksCallback, null);
    }
    
    ChildrenCallback tasksCallback = new ChildrenCallback(){
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                
                break;
            case OK:
                LOG.info("Getting tasks for recovery");
                tasks = children;
                getAssignedWorkers();
                
                break;
            default:
                LOG.error("getChildren failed, {}, {}", Code.get(rc), path);
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };
    
    private void getAssignedWorkers(){
        zk.getChildren(ZKNodeConst.ASSIGN_PARENT_NODE, false, assignedWorkersCallback, null);
    }
    
    ChildrenCallback assignedWorkersCallback = new ChildrenCallback(){
        public void processResult(int rc, String path, Object ctx, List<String> children){    
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getAssignedWorkers();
                
                break;
            case OK:  
                assignedWorkers = children;
                getWorkers(children);

                break;
            default:
                LOG.error("getChildren failed, {}, {}", Code.get(rc), path);
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };
        
    private void getWorkers(Object ctx){
        zk.getChildren(ZKNodeConst.WORKER_PARENT_NODE, false, workersCallback, ctx);
    }
    
    
    ChildrenCallback workersCallback = new ChildrenCallback(){
        public void processResult(int rc, String path, Object ctx, List<String> children){    
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getWorkers(ctx);
                break;
            case OK:
                LOG.info("Getting worker assignments for recovery: " + children.size());
                
                /*
                 * No worker available yet, so the master is probably let's just return an empty list.
                 */
                if(children.size() == 0) {
                    LOG.warn( "Empty list of workers, possibly just starting" );
                    cb.recoveryComplete(RecoveryCallback.OK, new ArrayList<String>());
                    
                    break;
                }
                
                /*
                 * Need to know which of the assigned workers are active.
                 */
                        
                activeWorkers = children;
                
                for(String s : assignedWorkers){
                    getWorkerAssignments(ZKNodeConst.ASSIGN_PARENT_NODE + "/" + s);
                }
                
                break;
            default:
                LOG.error("getChildren failed, {}, {}", Code.get(rc), path);
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };
    
    private void getWorkerAssignments(String s) {
        zk.getChildren(s, false, workerAssignmentsCallback, null);
    }
    
    ChildrenCallback workerAssignmentsCallback = new ChildrenCallback(){
        public void processResult(int rc, 
                String path, 
                Object ctx, 
                List<String> children) {    
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getWorkerAssignments(path);
                break;
            case OK:
            	LOG.info("workerAssignmentsCallback, path:{}, children:{}", path, children);
                String worker = path.replace(ZKNodeConst.ASSIGN_PARENT_NODE + "/", "");
                
                /*
                 * If the worker is in the list of active
                 * workers, then we add the tasks to the
                 * assignments list. Otherwise, we need to 
                 * re-assign those tasks, so we add them to
                 * the list of tasks.
                 */
                if(activeWorkers.contains(worker)) {
                    assignments.addAll(children);
                } else {
                    if (children.size() <= 0) {
						LOG.info("workerAssignmentsCallback, delete path:{}", path);
						deleteAssignment(path);
					}
                    
                    for( String task : children ) {
                        if(!tasks.contains( task )) {
                            tasks.add( task );
                            getDataReassign( path + "/" + task, task );
                        } else {
                            /*
                             * If the task is still in the list
                             * we delete the assignment.
                             */
                            deleteAssignment(path + "/" + task);
                        }
                        
                        /*
                         * Delete the assignment parent. 
                         */
                        deleteAssignment(path);
                    }
                }
                   
                assignedWorkers.remove(worker);
                
                /*
                 * Once we have checked all assignments,
                 * it is time to check the status of tasks
                 */
                if(assignedWorkers.size() == 0){
                    LOG.info("Getting statuses for recovery");
                    getStatuses();
                 }
                
                break;
            case NONODE:
                LOG.info( "No such znode exists: " + path );
                
                break;
            default:
                LOG.error("getChildren failed, {}, {}", Code.get(rc), path);
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };
    
    /**
     * Get data of task being reassigned.
     * 
     * @param path
     * @param task
     */
    void getDataReassign(String path, String task) {
    	
    	LOG.info("getDataReassign, path:{}, task:{}", path, task);
    	
        zk.getData(path, 
                false, 
                getDataReassignCallback, 
                task);
    }
    
    /**
     * Context for recreate operation.
     *
     */
    class RecreateTaskCtx {
        String path; 
        String task;
        byte[] data;
        
        RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }

    /**
     * Get task data reassign callback.
     */
    DataCallback getDataReassignCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
        	LOG.info("getDataReassignCallback, code:" + Code.get(rc) + ", path:{}, datalen:{}", path, data.length);
        	
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getDataReassign(path, (String) ctx); 
                
                break;
            case OK:
            	
            	byte[] nodeData;
				try {
					nodeData = GzipUtil.ungzip(data);
					recreateTask(new RecreateTaskCtx(path, (String) ctx, nodeData));
				} catch (Exception e) {
					e.printStackTrace();
				}            	
                
                break;
            default:
                LOG.error("Something went wrong when getting data, {}, {}", Code.get(rc), path);
            }
        }
    };
    
    /**
     * Recreate task znode in /tasks
     * 
     * @param ctx Recreate text context
     */
    void recreateTask(RecreateTaskCtx ctx) {
    	
		try {
			byte[] data;
			data = GzipUtil.gzip(ctx.data);
	        zk.create(ZKNodeConst.TASK_PARENT_NODE + "/" + ctx.task,
	                data,
	                Ids.OPEN_ACL_UNSAFE, 
	                CreateMode.PERSISTENT,
	                recreateTaskCallback,
	                ctx);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
    /**
     * Recreate znode callback
     */
    StringCallback recreateTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                recreateTask((RecreateTaskCtx) ctx);
       
                break;
            case OK:
                deleteAssignment(((RecreateTaskCtx) ctx).path);
                
                break;
            case NODEEXISTS:
                LOG.warn("Node shouldn't exist: " + path);
                
                break;
            default:
                LOG.error("Something wwnt wrong when recreating task, {}, {}", Code.get(rc), path);
            }
        }
    };
    
    /**
     * Delete assignment of absent worker
     * 
     * @param path Path of znode to be deleted
     */
    void deleteAssignment(String path){
    	LOG.info("deleteAssignment, path:{}", path);
        zk.delete(path, -1, taskDeletionCallback, null);
    }
    
    VoidCallback taskDeletionCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteAssignment(path);
                break;
            case OK:
                LOG.info("Task correctly deleted: " + path);
                break;
            default:
                LOG.error("Failed to delete task data, {}, {}", Code.get(rc), path);
            } 
        }
    };
    
    
    void getStatuses(){
        zk.getChildren(ZKNodeConst.STATUS_PARENT_NODE, false, statusCallback, null); 
    }
    
    ChildrenCallback statusCallback = new ChildrenCallback(){
        public void processResult(int rc, 
                String path, 
                Object ctx, 
                List<String> children){    
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getStatuses();
                
                break;
            case OK:
                LOG.info("Processing assignments for recovery");
                status = children;
                processAssignments();
                
                break;
            default:
                LOG.error("getChildren failed, {}, {}", Code.get(rc), path);
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };
    
    private void processAssignments(){
        LOG.info("Size of tasks: " + tasks.size());
        // Process list of pending assignments
        for(String s: assignments){
            LOG.info("Assignment: " + s);
            deleteAssignment(ZKNodeConst.TASK_PARENT_NODE + "/" + s);
            tasks.remove(s);
        }
        
        LOG.info("Size of tasks after assignment filtering: " + tasks.size());
        
        for(String s: status){
            LOG.info( "Checking task: {} ", s );
            deleteAssignment(ZKNodeConst.TASK_PARENT_NODE + "/" + s);
            tasks.remove(s);
        }
        LOG.info("Size of tasks after status filtering: " + tasks.size());
        
        // Invoke callback
        cb.recoveryComplete(RecoveryCallback.OK, tasks);     
    }
    
	public static void main(String[] args) {
	}
}
