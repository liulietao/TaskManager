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

/**
 * @author liulietao
 *
 */
public class TaskProducer implements Watcher, Closeable {
	
	private static final Logger LOG = LoggerFactory.getLogger(TaskProducer.class);
	
	private ZooKeeper zk;
	private String hostPort;

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
	
	
	/* (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(WatchedEvent arg0) {
		// TODO Auto-generated method stub
		LOG.info("process");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
