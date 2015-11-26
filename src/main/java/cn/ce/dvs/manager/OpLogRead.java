package cn.ce.dvs.manager;

import static cn.ce.dvs.manager.DVSContext.getDBC;
import static cn.ce.dvs.manager.DVSContext.mongos;
import static cn.ce.dvs.manager.DVSContext.run;
import static cn.ce.dvs.manager.DVSContext.writeQueueList;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.BSONTimestamp;

import cn.ce.dvs.config.DataSource;
import cn.ce.dvs.config.SubtaskConfig;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class OpLogRead implements Callable<Object>{
	
	private static Log log = LogFactory.getLog(OpLogRead.class);
	
	private long count = 0;
	private DBCollection sourceDBC = null;
	private DBCollection checkPointDBC= null;
	
	private BSONTimestamp start = null;
	private SubtaskConfig tableConfig = null;
	private String ns = null;
	
	public OpLogRead(SubtaskConfig tableConfig) {
		this.tableConfig = tableConfig;
		this.sourceDBC = getDBC(tableConfig.getSourceDataSource().getDataSourceId(),"local",tableConfig.getReadMode().getValue(),mongos);
//		this.sourceDBC = getDBC(tableConfig.getSourceDataSource(),mongos);
		this.checkPointDBC = getDBC(tableConfig.getCheckPointDataSource(),mongos);
		this.ns = tableConfig.getSourceDataSource().getDbName()+"."+tableConfig.getSourceDataSource().getTableName();
	}
	
	private String getPositionKey(SubtaskConfig tcf){
		return tcf.getId()+"#"+count;
	}
	
	private int getHash(DBObject dbo,SubtaskConfig tc){
		String key = null;
//		if(tc.getSynType().equals(SynType.c) || (tc.getSynType().equals(SynType.f) && tc.isMaster())){
//			key =  tc.getSourceMasterKey();
//		}else{
//			key = tc.getSourceSlaveKey();
//		}
		key =  tc.getSourceMasterKey();
		Object val = dbo.get(key);
		
//		String tmp = tc.getId()+"#"+val.toString();
		String tmp = val.toString();
		return tmp.hashCode();
	}
	
	private BlockingQueue<Action> getQueue(int hash){
		return  writeQueueList[Math.abs(hash) % writeQueueList.length];
	}
	
	private OPType getOPType(DBObject dbo){
		Object op = dbo.get("dvs_mysql_op_type") == null ?  dbo.get("dmlType"):dbo.get("dvs_mysql_op_type");
		op = op==null?dbo.get("op"):op;
		if(op == null) return null;
		
		if(op.toString().equalsIgnoreCase(OPType.insert.getKey())||op.toString().equalsIgnoreCase("i")){
			return OPType.insert;
		}
		if(op.toString().equalsIgnoreCase(OPType.update.getKey())||op.toString().equalsIgnoreCase("u")){
			return OPType.update;
		}
		if(op.toString().equalsIgnoreCase(OPType.delete.getKey())||op.toString().equalsIgnoreCase("d")){
			return OPType.delete;
		}
		
		return null;
	}
	
	
	private DBObject opLogToDBO(DBObject dbo){

		if(dbo instanceof BasicDBObject){
			
			BasicDBObject opLog = (BasicDBObject)dbo;
			
			String op = opLog.getString("op");
			
//			if(StringUtils.isBlank(op)){
//				log.error("no op :"+ opLog);
//				return null;
//			}
			
			if(!("i".equals(op) || "u".equals(op))){
				return null;
			}
			
//			String ns = opLog.getString("ns");
//			if(StringUtils.isBlank(ns)){
//				log.error("no ns :"+ opLog);
//				return null;
//			}
			
//			String tmp = tableConfig.getSourceDataSource().getDbName()+"."+tableConfig.getSourceDataSource().getTableName();
//			if(!tmp.equals(ns)){
//				return null;
//			}
			
			DBObject targetDBO = (DBObject) opLog.get("o");
			
			if(targetDBO == null){
				log.error("o is null :" + opLog);
				return null;
			}
			//如果op log 是set方式，则查询原表
			if(targetDBO.get("$set") != null){
				log.info("$set :" + opLog);
				DBCollection dbc =  getDBC(tableConfig.getSourceDataSource(),mongos);
				BasicDBObject o2 = (BasicDBObject) opLog.get("o2");
				targetDBO = dbc.findOne(o2);
				if(targetDBO == null){
					log.error("not found o2 :" + opLog);
				}
			}
			targetDBO.put("op", op);
			return targetDBO;
			
		}else{
			log.error("Not BasicDBObject instance"+ dbo);
			return null;
		}
	}
	
	private Action dboToAction(DBObject opLog){
		
		DBObject dbo = opLogToDBO(opLog);
		if(dbo == null){
			return null;
		}
		
		String sourceMasterKey = tableConfig.getSourceMasterKey();
//		if(dbo.get("CUSTID") == null){
//			log.error("数据异常，无主键信息 :"+ dbo);
//			return null;
//		}
		if(dbo.get(sourceMasterKey) == null){
			log.error("数据异常，无主键信息 :"+ dbo);
			return null;
		}
		
		if(!tableConfig.isMaster() && dbo.get(tableConfig.getSourceSlaveKey()) == null){
			log.error("数据异常，无从表主键信息 :"+ dbo);
			return null;
		}
		
		Action action = new Action();
		action.setPositionKey(getPositionKey(tableConfig));
//		action.setPostion((BSONTimestamp) dbo.get("dvs_server_ts"));
		action.setPostion((BSONTimestamp) opLog.get("ts"));
		action.setSourceDBO(dbo);
//		getHash(dbo.get("CUSTID"),tableConfig);
		int hash = getHash(dbo,tableConfig);
		action.setHash(hash);
		action.setTableConfig(tableConfig);
		OPType opType = getOPType(dbo);
		if(opType == null){
			log.error("数据异常，无opType :"+ dbo);
			return null;
		}
		
		action.setOpType(opType);
		
		count++;
		return action;
	} 
	
	//构造游标
	private DBCursor getDBCursor(){
		DBObject query = null;
		DBCursor cur = null;
		if(this.start == null){
			query = QueryBuilder.start("ns").is(ns).get();
			cur = sourceDBC.find(query);
			cur.addOption(Bytes.QUERYOPTION_TAILABLE);
			cur.addOption(Bytes.QUERYOPTION_AWAITDATA);
		}else{
			query = QueryBuilder.start("ts").greaterThan(this.start).and("ns").is(ns).get();
			cur = sourceDBC.find(query);
			cur.addOption(Bytes.QUERYOPTION_TAILABLE);
			cur.addOption(Bytes.QUERYOPTION_AWAITDATA);
			cur.addOption(Bytes.QUERYOPTION_OPLOGREPLAY);
		}
		
		return cur;
	}
	
	private  void getAndPut() throws InterruptedException{
		long tt = System.currentTimeMillis();
		DBCursor c = getDBCursor();
		DBObject orderBy = new BasicDBObject("$natural", 1);
		c.sort(orderBy);
		DBObject last = null;
		
		while (c.hasNext()){
			last = (DBObject) c.next();
			Action action = dboToAction(last);
			if(action == null){
				continue;
			}
			getQueue(action.getHash()).put(action);
			this.start = (BSONTimestamp) last.get("ts");
			
			if(count%5000 == 0){
				tt = System.currentTimeMillis() - tt;
				log.info(tableConfig.getId()+ " read:" + " elapsed time "+tt+" ms," + "size "+5000);
				tt = System.currentTimeMillis();
			}
		}
	};
	
	//获取起始点
	private BSONTimestamp getPosition(){
		//首先从检查点查询
		DBObject o = checkPointDBC.findOne(new BasicDBObject("_id",tableConfig.getId()));
		BSONTimestamp r = null;
		if(o == null){
			//查询不到，取op log 最后的建表时间
			DataSource sds = tableConfig.getSourceDataSource();
			DBObject query = QueryBuilder.start("op").is("c").and("ns").is(sds.getDbName()+".$cmd").and("o").is(new BasicDBObject("create",sds.getTableName())).get();
			DBObject orderBy =  new BasicDBObject("$natural", -1);
			o = sourceDBC.findOne(query, null, orderBy );
			if(o != null){
			  r = (BSONTimestamp) o.get("ts");
			}
		}else{
			r = (BSONTimestamp) o.get("cp");
		}
		
		return r;
	}
	
	public Object call() throws Exception {
		BSONTimestamp p = getPosition();
		start = p;
		try {
			while (run) {
				getAndPut();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			Thread.interrupted();
		}
		return null;
	}
}