/**
 * 
 */
package com.youku.opencloud.taskmanager;

import java.io.IOException;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.callback.OnProducerCallback;
import com.youku.opencloud.constant.ZKNodeConst;
import com.youku.opencloud.dto.TaskDto;
import com.youku.opencloud.util.GzipUtil;

/**
 * @author liulietao
 *
 */
public class ProducerClient extends BaseZKClient {
	
	private static final Logger log = LoggerFactory.getLogger(ProducerClient.class);

	private OnProducerCallback producerCallback;
	
	/**
	 * @param zkHost
	 */
	public ProducerClient(String zkHost, OnProducerCallback callback) {
		super(zkHost);
		
		producerCallback = callback;
	}
	
	public void bootstrap() throws IOException {
		log.info("bootstrap");
		startZK();
	}
	
	public void close() {
		log.info("close");
		try {
			stopZK();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void process(WatchedEvent e) {
		super.process(e);
		
		if (isConnected()) {
			producerCallback.onConnectedSuccess();
		} else {
			producerCallback.onConnectedFailed();
		}
	}
	
	public void createTask(TaskDto taskCtx) {
		log.info("createTask, task:{}, data:{}", taskCtx.getTaskName(), new String(taskCtx.getData()));
		
		byte[] data;
		try {
			data = GzipUtil.gzip(taskCtx.getData());
	        zk.create(ZKNodeConst.TASK_PARENT_NODE + "/task-", 
	        		data,
	                Ids.OPEN_ACL_UNSAFE,
	                CreateMode.PERSISTENT_SEQUENTIAL,
	                createTaskCallback,   
	                taskCtx);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("createTask, " + e);
		}
	}
	
    StringCallback createTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
        	log.info("createTaskCallback, {}, {}", Code.get(rc), name);
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
            	createTask((TaskDto) ctx);
                
                break;
            case OK:
                log.info("createTaskCallback, My created task name: {}", name);
                ((TaskDto) ctx).setTaskName(name);
                
                producerCallback.onSummitTaskResult(true, (TaskDto) ctx);
                break;
            default:
                log.error("createTaskCallback, Something went wrong, {}, {}", Code.get(rc), path);
                producerCallback.onSummitTaskResult(false, (TaskDto) ctx);
            }
        }
    };
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}
}
