package cn.ce.dvs.manager;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZookeeperMasterLeaderLatch implements HAManager,/*ConnectionStateListener,*/LeaderLatchListener{
	
	private volatile boolean isRun = false; 
	
	private Log log = LogFactory.getLog(getClass());
	
	private static final String DEFAULT_BASEPATH = "dvs";
	private static final String DEFAULT_SERVICE_NAME = "dataAssembly";
	
	public static final int DEFAULT_SESSION_TIMEOUT_MS = 15 * 1000;
	public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 15 * 1000;
	public static final int DEFAULT_MAX_RETRIES = 3;
	
	
	private CuratorFramework client = null;
	private String serviceName =DEFAULT_SERVICE_NAME;
	private String basePath = DEFAULT_BASEPATH;
	private String zkConnectString = null;
	
	private int sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;
	private int connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;
	private int maxRetries = DEFAULT_MAX_RETRIES;
	
	private LeaderLatch leader;
	private String id = null;
	private String latchPath = null;
	
	private Bootstrap bootstrap = null;
	
	private String getPath(String serviceName){
		return "/"+getBasePath()+"/" + serviceName;
	}
	
	public ZookeeperMasterLeaderLatch(String zkConnectString,String id,Bootstrap bootstrap){
		this.zkConnectString = zkConnectString;
		this.id = id;
		this.bootstrap = bootstrap;
		client = CuratorFrameworkFactory
				.newClient(getZkConnectString(),getSessionTimeoutMs() ,getConnectionTimeoutMs(),new ExponentialBackoffRetry(1000, getMaxRetries()));
		latchPath = getPath(serviceName);
//		client.getConnectionStateListenable().addListener(this);
	}
	
	public void start(){
		try {
			client.start();
			client.blockUntilConnected();
			
			leader = new LeaderLatch(client, latchPath,this.id);
			leader.start();
			leader.addListener(this);
			
			isRun = true;
			leader.await();
			
			while (isRun) {
				if(leader.hasLeadership()){
					bootstrap.startNoHA();
				}else{
					TimeUnit.MILLISECONDS.sleep(2000);
				}
			}
			
		} catch (Throwable e) {
			throw new RuntimeException("zookeeper HA manager start failed",e);
		}
	}
	

	public String getZkConnectString() {
		return zkConnectString;
	}

	public void setZkConnectString(String zkConnectString) {
		this.zkConnectString = zkConnectString;
	}

	public String getBasePath() {
		return basePath;
	}

	public void setBasePath(String basePath) {
		this.basePath = basePath;
	}

	public int getSessionTimeoutMs() {
		return sessionTimeoutMs;
	}

	public void setSessionTimeoutMs(int sessionTimeoutMs) {
		this.sessionTimeoutMs = sessionTimeoutMs;
	}

	public int getConnectionTimeoutMs() {
		return connectionTimeoutMs;
	}

	public void setConnectionTimeoutMs(int connectionTimeoutMs) {
		this.connectionTimeoutMs = connectionTimeoutMs;
	}

	public int getMaxRetries() {
		return maxRetries;
	}

	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}
	
	@Override
	public void close() {
		isRun = false;
		if(leader != null){
			try {
				leader.close();
			} catch (IOException e) {
				log.error(e.getMessage(), e);
			}
		}
		
		if(client != null){
			client.close();
		}
	}

//	@Override
//	public void stateChanged(CuratorFramework client, ConnectionState newState) {
//		System.out.println(newState);
//	}

	@Override
	public void isLeader() {
//		System.out.println("isLeader");
		log.info("isLeader");
	}

	@Override
	public void notLeader() {
		log.info("notLeader");
		bootstrap.stop0();
	}
}
