/**
 * 
 */
package com.youku.opencloud.TaskManager;

import java.io.Closeable;
import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.*;

import com.youku.opencloud.Util.OSUtils;

/**
 * @author liulietao
 *
 */
public class TaskProducer implements Watcher, Closeable {
	
	private static final Logger log = LoggerFactory.getLogger(TaskProducer.class);
	
	private ZooKeeper zk;
	private String hostPort;
	
	private volatile boolean connected = false;
	private volatile boolean expired = false;

	/**
	 * Create a new task producer
	 * 
	 */
	TaskProducer(String hostPort) {
		this.hostPort = hostPort;
	}
	
	void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 3000, this);
	}
	
	void stopZK() throws IOException, InterruptedException {
		zk.close();
	}
	
    /**
     * Check if this client is connected.
     * 
     * @return boolean ZooKeeper client is connected
     */
    boolean isConnected() {
        return connected;
    }
    
    /**
     * Check if the ZooKeeper session has expired.
     * 
     * @return boolean ZooKeeper session has expired
     */
    boolean isExpired() {
        return expired;
    }	
	
	/* (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		log.info("close");
	}

	/* (non-Javadoc)
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(WatchedEvent e) {
		// TODO Auto-generated method stub
		log.info("process event, type:{}, state:{}", e.getType(), e.getState());
		
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

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException, InterruptedException {
		TaskProducer p = new TaskProducer(args[0]);
		p.startZK();
		
		while (! p.isConnected()) {
			Thread.sleep(100);
		}
		
		int cpuLoad = OSUtils.cpuUsage();
		log.info("cpu load : {}", cpuLoad);
		
		p.stopZK();
	}

}
