package cn.ce.dvs.manager;

import static cn.ce.dvs.manager.DVSContext.getDBC;
import static cn.ce.dvs.manager.DVSContext.getDB;
import static cn.ce.dvs.manager.DVSContext.mongos;
import static cn.ce.dvs.manager.DVSContext.solrs;
import static cn.ce.dvs.manager.DVSContext.positions;
import static cn.ce.dvs.manager.DVSContext.run;
import static cn.ce.dvs.manager.DVSContext.writeQueueList;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.MDC;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.bson.types.BSONTimestamp;

import sun.misc.Signal;
import cn.ce.dvs.config.ConfigManager;
import cn.ce.dvs.config.DataSource;
import cn.ce.dvs.config.DataSourceType;
import cn.ce.dvs.config.SubtaskConfig;
import cn.ce.dvs.config.XmlBasedConfigManager;
import cn.ce.utils.Utils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

@SuppressWarnings(value={"unchecked","rawtypes"})
public class Bootstrap implements Observer {
	
	private static Log log = LogFactory.getLog(Bootstrap.class);
	
	//非写工作线程池(包括读、状态监控、写检查点线程)
	private ExecutorService readAndOtherPool = null;
	//写线程
	private ExecutorService writePool = null;
	
	private CompletionService<Object> writeCompletionService = null;
	private CompletionService<Object> readAndOtherService = null;
	//子任务列表
	private List<SubtaskConfig> configList = null;
	//配置管理器
	private ConfigManager configManager = null;
	
	//高可用管理
	private HAManager haManager = null;
	
//	private boolean isInitConfig = false;
	
	public Bootstrap(){
		this.configManager = new XmlBasedConfigManager(null);
	}
	
	public Bootstrap(ConfigManager configManager){
		this.configManager = configManager;
	}
	
	public ConfigManager getConfigManager() {
		return configManager;
	}

	public void setConfigManager(ConfigManager configManager) {
		this.configManager = configManager;
	}
	
	private void initMongo(DataSource ds) throws Exception{
		String dsid = ds.getDataSourceId();
		if(mongos.get(dsid) == null){
			String uri = ds.getUri();
			MongoClientURI mcuri = new MongoClientURI(uri);
			mongos.put(dsid, new MongoClient(mcuri));
		}
	};
	
	private void initSolr(DataSource ds) throws Exception{
		String dsid = ds.getDataSourceId();
		if(solrs.get(dsid) == null){
			String uri = ds.getUri();
			CommonsHttpSolrServer solr = new CommonsHttpSolrServer(uri);
			solr.setSoTimeout(10000);
			solr.setConnectionTimeout(10000);
			solr.setDefaultMaxConnectionsPerHost(50);
			solr.setMaxRetries(5);
			solr.setMaxTotalConnections(100);
			solrs.put(dsid, solr);
		}
	};
	
	
	
	
	private void initParams(){
		log.info("参数初始化......");
		run = true;
		readAndOtherPool = Executors.newCachedThreadPool();
		readAndOtherService = new ExecutorCompletionService (readAndOtherPool);
		positions = new ConcurrentHashMap<String, BSONTimestamp>();
		
		writeQueueList = new ArrayBlockingQueue[configManager.getWritePoolSize()];
		 for (int i = 0; i < writeQueueList.length; i++) {
			 writeQueueList[i] = new ArrayBlockingQueue<Action>(5000);
	 	}
		
		writePool  = Executors.newFixedThreadPool(configManager.getWritePoolSize());
		writeCompletionService = new ExecutorCompletionService(writePool);
		log.info("初始化完成......");
	};
	
	
	private void checkSourceTable(DataSource ds){
		if(DataSourceType.mongo == ds.getDataSourceType()){
			DB s = getDB(ds, mongos);
			if(!s.collectionExists(ds.getTableName())){
				throw new RuntimeException("The data source "+ ds.getDbName()+" "+ ds.getTableName()+ " does not exist");
			}
		}
	}
	
	private void initDataSource(DataSource ... ds) throws Exception{
		for (DataSource dataSource : ds) {
			DataSourceType dst = dataSource.getDataSourceType();
			switch (dst) {
			case mongo:
				initMongo(dataSource);
				break;
			case solr:
				initSolr(dataSource);
				break;
			default:
				throw new RuntimeException("Unsupported data source type :"+ dst);
			}
		}
	}
	
	private void initConfig() throws Exception{
		log.info("配置初始化......");
		configList = configManager.loadConfig();
		
		if(configList == null || configList.size() <=0 ){
			throw new RuntimeException("初始化失败，未获取配置信息");
		}
		
		SignalObserverHandler sh = new SignalObserverHandler();
         sh.addObserver(this);
         sh.handleSignal("INT");
         sh.handleSignal("TERM");
         
         mongos.clear();
         solrs.clear();
         
		for (SubtaskConfig tableConfig : configList) {
			    DataSource ds = tableConfig.getSourceDataSource();
			    initDataSource(ds,tableConfig.getTargetDataSource(),tableConfig.getCheckPointDataSource());
				//检查原表是否存在
				checkSourceTable(ds);
		}
		
		
		log.info("create index begin");
		for (SubtaskConfig tableConfig : configList) {
			if(tableConfig.getTargetDataSource().getDataSourceType() == DataSourceType.mongo){
				DBCollection dbc = getDBC(tableConfig.getTargetDataSource(), mongos);
				DBObject keys = new BasicDBObject(tableConfig.getTargetMasterKey(),Order.ASC.getKey());
				DBObject options = new BasicDBObject("name",tableConfig.getTargetMasterKey()+"_index");
				options.put("background", true);
				dbc.createIndex(keys,options );
				
				if(tableConfig.getTargetSlaveColName() != null && tableConfig.getTargetSlaveKey() != null){
					keys = new BasicDBObject(tableConfig.getTargetSlaveColName()+"."+ tableConfig.getTargetSlaveKey(),Order.ASC.getKey());
					options = new BasicDBObject("name",tableConfig.getTargetSlaveColName()+"_"+ tableConfig.getTargetSlaveKey()+"_index");
					options.put("background", true);
					dbc.createIndex(keys,options );
				}
			}
		}
		log.info("create index end");
	}
	
	private static Multimap<String, SubtaskConfig> map = ArrayListMultimap.create();
	
	private void start0()throws InterruptedException{

		log.info("启动中......");
		for (SubtaskConfig tableConfig : configList) {
			if(tableConfig.getReadMode() != null && (tableConfig.getReadMode().name().startsWith("opLog"))){
				DataSource sds = tableConfig.getSourceDataSource();
				String dsType = sds.getDataSourceType().name();
				String dsid = sds.getDataSourceId();
				
				String key = Utils.concat("_",dsType,dsid,tableConfig.getReadMode().name());
				map.put(key, tableConfig);
//				readAndOtherService.submit(new OpLogRead(tableConfig));
			}else{
				readAndOtherService.submit(new Read(tableConfig));
			}
		}
		
		for (Entry<String, Collection<SubtaskConfig>> entry : map.asMap().entrySet()) {
			if(entry.getKey().startsWith(DataSourceType.mongo.name())){
				readAndOtherService.submit(new SingleOpLogRead((List)(entry.getValue())));
			}
		}
		
		
		for (int i = 0; i < configManager.getWritePoolSize(); i++) {
			writeCompletionService.submit(new Write(writeQueueList[i]));
		}
		
		readAndOtherService.submit(new Monitor());
		readAndOtherService.submit(new State(configList));
		
		boolean isDone = false;
		boolean isError = false;
		while (true) {
			Future<?> readAndOtherFuture = readAndOtherService.poll();
			Future<?> writeFuture = writeCompletionService.poll();
			
			if(readAndOtherFuture != null){
				try {
					readAndOtherFuture.get();
				} catch (ExecutionException e) {
					isError = true;
					log.error("read thread or other thread error", e);
				}finally{
					isDone = true;
				}
			}
			
			if(writeFuture != null){
				try {
					writeFuture.get();
				} catch (ExecutionException e) {
					isError = true;
					log.error("write thread error", e);
				}finally{
					isDone = true;
				}
			}
			
			if(isDone){
				break;
			}else{
				TimeUnit.MILLISECONDS.sleep(1000);
			}
		}
		
		if(isError){
			throw new RuntimeException("work thread error");
		}
	
	}
	
	public void startNoHA() throws Throwable{
		//配置初始化
		initConfig();
		//参数初始化
		initParams();
		try {
			start0();
		} catch (Exception e) {
			log.error("异常中断，重新启动中......",e);
			reStart0(true,5000);
		}
	}
	
	public void start() throws Throwable{
		if(haManager != null){
			haManager.start();
		}else{
			startNoHA();
		}
	};
	
	private void reStart0(boolean isLooping,long sleep){
			try {
				log.info("重新启动中......");
				stop0();
				TimeUnit.MILLISECONDS.sleep(sleep);
				//参数初始化
				initParams();
				start0();
			} catch (Exception e) {
				log.error(e);
				if(isLooping){
					log.warn("异常中断，重新启动中......");
					reStart0(isLooping,sleep);
				}else{
					throw new RuntimeException(e);
				}
			}
	};
	
//	public void reStart(long sleep){
//			reStart0(false,sleep);
//	}
	
	public void stop0(){
		run = false;
		if(readAndOtherPool != null){
			readAndOtherPool.shutdownNow();
		}
		
		if(writePool != null){
			writePool.shutdownNow();
		}
	};
	
	
	public void stop(){
		if(haManager != null){
			haManager.close();
		}
		
		stop0();
		for (Entry<String, MongoClient> m: mongos.entrySet()){
			try {
				m.getValue().close();
			} catch (Exception e) {
				log.warn(e.getMessage(), e);
			}
		}
		mongos.clear();
		solrs.clear();
	}
	
	//java -jar dvs.jar -zk 10.12.32.134:2181
	public static void main(String[] args) throws InterruptedException, ParseException {
		
		String zk = "zk";
		String st = "st";
		String ct = "ct";
		String maxRetries = "maxRetries";
		
		Options options = new Options();
		options.addOption("h", false, "help list");
		
		//程序参数，xml 配置路径
		options.addOption("p", true, "config path");
		
		options.addOption(zk,true,"Zookeeper connect string");
		options.addOption(st,true,"Zookeeper session timeout, default value is " + ZookeeperMasterLeaderLatch.DEFAULT_SESSION_TIMEOUT_MS+ " ms");
		options.addOption(ct,true,"Zookeeper connection timeout, default value is " + ZookeeperMasterLeaderLatch.DEFAULT_CONNECTION_TIMEOUT_MS+ " ms");
		options.addOption(maxRetries,true,"Zookeeper max number of times to retry, default value is " + ZookeeperMasterLeaderLatch.DEFAULT_MAX_RETRIES);
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		//help 帮助
		if(cmd.hasOption("h")){
			 HelpFormatter hf = new HelpFormatter(); 
			 hf.printHelp("Options", options); 
			 return;
		}
		
		//xml配置路径
		String path = null;
		if(cmd.hasOption("p")){
			path = cmd.getOptionValue("p");
			if(StringUtils.isBlank(path)){
				throw new IllegalArgumentException("config path must not be null, empty, or blank");
			}
		}
		
		//获取ip
		Set<String> set = Utils.getIpV4();
		if(set == null || set.isEmpty()){
			log.error("获取ip失败");
		}else{
			MDC.put("ip", set.toString());
		}
		
		try {
			//写pid
			Utils.writePid();
		} catch (Throwable e) {
			log.error(e,e);
		}
		
		//构造配置管理器
		ConfigManager cm = new XmlBasedConfigManager(path);
		//构造引导程序
		Bootstrap bts = new Bootstrap(cm);
		if(cmd.hasOption(zk)){
			String id = DigestUtils.md5Hex(((XmlBasedConfigManager)cm).getConfigPath());
			ZookeeperMasterLeaderLatch z = new ZookeeperMasterLeaderLatch(cmd.getOptionValue(zk),id,bts);
			
			if(cmd.hasOption(st)){
				int sessionTimeoutMs = Integer.parseInt(cmd.getOptionValue(st));
				z.setSessionTimeoutMs(sessionTimeoutMs);
			}
			if(cmd.hasOption(ct)){
				int connectionTimeoutMs = Integer.parseInt(cmd.getOptionValue(ct));
				z.setConnectionTimeoutMs(connectionTimeoutMs);
			}
			
			if(cmd.hasOption(maxRetries)){
				z.setMaxRetries(Integer.parseInt(cmd.getOptionValue(maxRetries)));
			}
			
			bts.setHaManager(z);
		}
			//启动
		try {
			bts.start();
		} catch (Throwable e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void update(Observable o, Object arg) {
		log.info("Received signal: " + arg);
		if(arg instanceof Signal && arg != null){
			Signal signal = (Signal)arg;
			int sn = signal.getNumber();
			log.info("signal name:"+ signal.getName()+" signal number:"+sn);
			if(sn == 2 || sn == 15){
				stop();
				Utils.removePidFile();
//				System.exit(0);
			}
		}
	}

	public HAManager getHaManager() {
		return haManager;
	}

	public void setHaManager(HAManager haManager) {
		this.haManager = haManager;
	}
}
