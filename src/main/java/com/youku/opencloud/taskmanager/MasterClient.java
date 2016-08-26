/**
 * 
 */
package com.youku.opencloud.taskmanager;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youku.opencloud.callback.OnManagerCallback;
import com.youku.opencloud.constant.ZKNodeConst;
import com.youku.opencloud.taskmanager.TaskRecover.RecoveryCallback;
import com.youku.opencloud.util.GzipUtil;

/**
 * @author liulietao
 *
 */
public class MasterClient extends ManagerClient {

	private static final Logger log = LoggerFactory.getLogger(MasterClient.class);
	
	enum MasterStates {RUNNING, ELECTED, NOTELECTED};
	private volatile MasterStates state = MasterStates.RUNNING;
	
	private Random random = new Random(this.hashCode());
	private String serverId = Integer.toHexString( random.nextInt() );
	
	
	/**
	 * @param zkHost
	 * @param cb
	 */
	public MasterClient(String zkHost, OnManagerCallback cb) {
		super(zkHost, cb);
		
		serverId = UUID.randomUUID().toString();
	}
	
    /*
     **************************************
     **************************************
     * Methods related to master election.*
     **************************************
     **************************************
     */
	public void runForMaster() {
		log.info("running for master");
		
		byte[] data;
		try {
			data = GzipUtil.gzip(serverId.getBytes());
			zk.create(ZKNodeConst.MASTER_PARENT_NODE, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL
					, masterCreateCallback, null);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("runForMaster, " + e);
		}
	}
	
	private StringCallback masterCreateCallback = new  StringCallback() {
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case OK:
				state = MasterStates.ELECTED;
				takeLeadership();
				break;
			case NODEEXISTS:
				state = MasterStates.NOTELECTED;
				masterExists();
				break;
			default:
				state = MasterStates.NOTELECTED;
				log.error("Something went wrong when running for master, {}, {}", Code.get(rc), path);
				break;
			}
			log.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
		}
	};
	
	private void takeLeadership() {
		log.info("take leader, get worker, get task");
		getWorkers();
		
		getTasksStatus();
		
		
        (new TaskRecover(zk)).recover( new RecoveryCallback() {
            public void recoveryComplete(int rc, List<String> tasks) {
                if(rc == RecoveryCallback.FAILED) {
                    log.error("Recovery of assigned tasks failed.");
                } else {
                    log.info( "Assigning recovered tasks" );
                    getTasks();
                }
            }
        });
	}
	
	private void masterExists() {
		log.info("watch on master node");
		zk.exists(ZKNodeConst.MASTER_PARENT_NODE, masterExistsWatcher, masterExistsCallback, null);
	}
	
	private Watcher masterExistsWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			if (e.getType() == EventType.NodeDeleted) {
				runForMaster();
			}
		}
	};
	
	private StatCallback masterExistsCallback = new StatCallback() {
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				masterExists();
				break;
			case OK:
				break;
			case NONODE:
				state = MasterStates.RUNNING;
				runForMaster();
				log.info("It seems master is gone, so let's run for master");
				break;
			default:
				checkMaster();
				break;
			}
		}
	};
	
	private void checkMaster() {
		log.info("check master node");
		zk.getData(ZKNodeConst.MASTER_PARENT_NODE, false, masterCheckCallback, null);
	}
	
	private DataCallback masterCheckCallback = new DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case NONODE:
				runForMaster();
				break;
			case OK:
				
				byte[] nodeData;
				try {
					nodeData = GzipUtil.ungzip(data);
					String nodeID   = new String(nodeData);
					
					if ( serverId.equals( nodeID )) {
						state = MasterStates.ELECTED;
						takeLeadership();
					} else {
						state = MasterStates.NOTELECTED;
						masterExists();
					}
				} catch (Exception e) {
					e.printStackTrace();
					log.error("masterCheckCallback, " + e);
				}

				break;
			default:
				log.error("error when check master, {}, {}", Code.get(rc), path);
				break;
			}
		}
	};

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
