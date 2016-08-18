/**
 * 
 */
package com.youku.opencloud.taskmanager;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.callback.OnConsumerCallback;

/**
 * @author liulietao
 *
 */
public class ConsumerClient extends BaseZKClient {

	private static final Logger log = LoggerFactory.getLogger(ConsumerClient.class);
	
	private OnConsumerCallback consumerCallback;
	
	private String serverId = Integer.toHexString((new Random()).nextInt());
	
	/**
	 * @param zkHost
	 */
	public ConsumerClient(String zkHost, OnConsumerCallback callback) {
		super(zkHost);
		
		consumerCallback = callback;
	}
	
	public void bootstrap() throws IOException {
		startZK();
	}
	
	@Override
	public void process(WatchedEvent e) {
		super.process(e);
		
		if (isConnected()) {
			consumerCallback.onConnectedSuccess();
			
			createAssignNode();
			
			register();
		} else {
			consumerCallback.onConnectedFailed();
		}
	}
	
    void createAssignNode(){
    	log.info("creating a /assign/worker-serverId parent znode to hold the tasks assigned to this worker");
        zk.create("/assign/worker-" + serverId, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                createAssignCallback, null);
    }
    
    StringCallback createAssignCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                createAssignNode();
                break;
            case OK:
                log.info("Assign node created");
                break;
            case NODEEXISTS:
                log.warn("Assign node already registered");
                break;
            default:
                log.error("Something went wrong: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };
	
    String name;

    /**
     * Registering the new worker, which consists of adding a worker
     * znode to /workers.
     */
    public void register(){
        name = "worker-" + serverId;
        log.info("Registering the new worker, /workers/{}", name);

        zk.create("/workers/" + name,
                "Idle".getBytes(), 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL,
                createWorkerCallback, null);
    }
    
    StringCallback createWorkerCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
                register();
                
                break;
            case OK:
                log.info("Registered successfully: " + serverId);
                
                break;
            case NODEEXISTS:
                log.warn("Already registered: " + serverId);
                
                break;
            default:
                log.error("Something went wrong: ", 
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
