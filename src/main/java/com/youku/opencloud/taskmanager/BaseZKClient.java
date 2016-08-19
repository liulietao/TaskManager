/**
 * 
 */
package com.youku.opencloud.taskmanager;

import java.io.Closeable;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liulietao
 *
 */
public class BaseZKClient implements Watcher, Closeable {
	
	private static final Logger log = LoggerFactory.getLogger(BaseZKClient.class);
	private String hostPort;
	protected ZooKeeper zk;
	
	private volatile boolean connected = false;
	private volatile boolean expired = false;
	
	private int sessionTimeout = 3000;

	/**
	 * 
	 */
	public BaseZKClient(String zkHost) {
		hostPort = zkHost;
	}

	protected void startZK() throws IOException {
		log.debug("");
		zk = new ZooKeeper(hostPort, sessionTimeout, this);
	}
	
	protected void stopZK() throws IOException, InterruptedException {
		log.debug("");
		zk.close();
	}
	
	protected boolean isConnected() {
		return connected;
	}
	
	protected boolean isExpired() {
		return expired;
	}
	
	/* (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		log.debug("close");
	}

	/* (non-Javadoc)
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(WatchedEvent e) {
		log.debug("process, ", e);
		
		if (e.getType() == Event.EventType.None) {
			switch (e.getState()) {
			case SyncConnected:
				connected = true;
				break;
			case Disconnected:
				connected = false;
				break;
			case Expired:
				expired = true;
				connected = false;
				log.error("session expired");
				break;
			default:
				break;
			}
		}
	}
	
	protected void createPath(String path, byte[] data) {
		log.info("createPath, {}", path);
		
		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createPathCallback, data);
	}

	private StringCallback createPathCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			log.debug("processResult, {}, {}",  KeeperException.create(Code.get(rc), path), name);
			
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
				createPath(path, (byte[]) ctx);
				break;
			case OK:
				log.info("Path created {}", path);
				break;
			case NODEEXISTS:
				log.info("Path already exist {}", path);
				break;
			default:
				log.error("error:{}", KeeperException.create(Code.get(rc), path));
				break;
			}
		}
	};
	
	protected void deletePath(String path) {
    	log.info("Delete path : {}", path);
    	
        zk.delete(path, -1, deletePathCallback, null);
	}
	
    private VoidCallback deletePathCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
        	log.debug("delete path : {}", KeeperException.create(Code.get(rc), path));
        	
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
            	deletePath(path);
                break;
            case OK:
                log.info("Task correctly deleted: " + path);
                break;
            default:
                log.error("Failed to delete task data" + 
                        KeeperException.create(Code.get(rc), path));
            } 
        }
    };
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		log.debug("main");
	}
}
