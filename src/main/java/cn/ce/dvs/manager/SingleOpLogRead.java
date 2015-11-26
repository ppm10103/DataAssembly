package cn.ce.dvs.manager;

import static cn.ce.dvs.manager.DVSContext.getDBC;
import static cn.ce.dvs.manager.DVSContext.mongos;
import static cn.ce.dvs.manager.DVSContext.run;
import static cn.ce.dvs.manager.DVSContext.writeQueueList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.BSONTimestamp;

import cn.ce.dvs.config.SubtaskConfig;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * 单线程读取OpLog
 * @author Administrator
 *
 */
public class SingleOpLogRead implements Callable<Object>{
	
	private static Log log = LogFactory.getLog(SingleOpLogRead.class);
	
	private Map<String, Long> countMap = new HashMap<String, Long>();
	private DBCollection sourceDBC = null;
//	private DBCollection checkPointDBC= null;
	
	private BSONTimestamp start = null;
	private List<SubtaskConfig> tableConfigList = null;
	private List<String> nsList = new ArrayList<String>();
	
	public SingleOpLogRead(List<SubtaskConfig> tableConfigList) {
		this.tableConfigList = tableConfigList;
		this.sourceDBC = getDBC(tableConfigList.get(0).getSourceDataSource().getDataSourceId(),"local",tableConfigList.get(0).getReadMode().getValue(),mongos);
//		this.sourceDBC = getDBC(tableConfig.getSourceDataSource(),mongos);
//		this.checkPointDBC = getDBC(tableConfig.getCheckPointDataSource(),mongos);
		for (SubtaskConfig subtaskConfig : tableConfigList) {
			nsList.add(subtaskConfig.getSourceDataSource().getDbName()+"."+subtaskConfig.getSourceDataSource().getTableName());
			countMap.put(subtaskConfig.getId(), State.BEGIN_NUM);
		}
		//this.ns = tableConfig.getSourceDataSource().getDbName()+"."+tableConfig.getSourceDataSource().getTableName();
	}
	
	private String getPositionKey(SubtaskConfig tcf){
		return tcf.getId()+"#"+countMap.get(tcf.getId());
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
	
	
	private DBObject opLogToDBO(DBObject dbo,SubtaskConfig tableConfig){

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
			log.error("Not a BasicDBObject instance :"+ dbo);
			return null;
		}
	}
	
	private List<Action> dboToActionList(DBObject opLog){
		List<Action> list = new ArrayList<Action>();
		BasicDBObject o = (BasicDBObject) opLog;
		String ns = o.getString("ns");
		for (SubtaskConfig subtaskConfig : tableConfigList) {
			String tmp = subtaskConfig.getSourceDataSource().getDbName()+"."+subtaskConfig.getSourceDataSource().getTableName();
			if(tmp.equals(ns)){
				Action action = dboToAction(opLog,subtaskConfig);
				if(action != null){
					list.add(action);
				}
			}
		}
		
		return list;
	}
	
	
	private Action dboToAction(DBObject opLog,SubtaskConfig tableConfig){
		
		DBObject dbo = opLogToDBO(opLog,tableConfig);
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
		
		long c = countMap.get(tableConfig.getId());
		c++;
		countMap.put(tableConfig.getId(), c);
		return action;
	} 
	
	//构造游标
	private DBCursor getDBCursor(){
		DBObject query = null;
		DBCursor cur = null;
		BasicDBList values = new BasicDBList();
		values.addAll(this.nsList);
		if(this.start == null){
//			query = QueryBuilder.start("ns").is(ns).get();
			query = QueryBuilder.start("ns").in(values).get();
			log.warn("任务没有设置checkpoint时间戳，程序自动寻找断点时间，可能查询时间超长");
			log.warn("query:"+query);
			cur = sourceDBC.find(query);
			cur.addOption(Bytes.QUERYOPTION_TAILABLE);
			cur.addOption(Bytes.QUERYOPTION_AWAITDATA);
		}else{
			query = QueryBuilder.start("ts").greaterThan(this.start).and("ns").in(values).get();
			cur = sourceDBC.find(query);
			cur.addOption(Bytes.QUERYOPTION_TAILABLE);
			cur.addOption(Bytes.QUERYOPTION_AWAITDATA);
			cur.addOption(Bytes.QUERYOPTION_OPLOGREPLAY);
		}
		
		return cur;
	}
	
	private  void getAndPut() throws InterruptedException{
//		long tt = System.currentTimeMillis();
		DBCursor c = getDBCursor();
		DBObject orderBy = new BasicDBObject("$natural", 1);
		c.sort(orderBy);
		DBObject last = null;
		
		while (c.hasNext()){
			last = (DBObject) c.next();
			List<Action> actionList = dboToActionList(last);
			if(actionList == null || actionList.size() == 0){
				continue;
			}
			
			for (Action action : actionList) {
				getQueue(action.getHash()).put(action);
			}
			
			this.start = (BSONTimestamp) last.get("ts");
			
//			if(count%5000 == 0){
//				tt = System.currentTimeMillis() - tt;
//				log.info(tableConfig.getId()+ " read:" + " elapsed time "+tt+" ms," + "size "+5000);
//				tt = System.currentTimeMillis();
//			}
		}
	};
	
	
	private BSONTimestamp minTimestamp(BSONTimestamp a,BSONTimestamp b){
		return a.compareTo(b) < 0 ? a : b;
	}
	
	//获取起始点
	private BSONTimestamp getPosition(){
		//首先从检查点查询
//		DBObject o = checkPointDBC.findOne(new BasicDBObject("_id",tableConfig.getId()));
//		DBObject o = null;
		BSONTimestamp r = null;
		
		for (SubtaskConfig cfg : tableConfigList) {
			DBCollection checkPointDBC = getDBC(cfg.getCheckPointDataSource(),mongos);
			DBObject tmp = checkPointDBC.findOne(new BasicDBObject("_id",cfg.getId()));
			if(tmp == null){
//				o = null;
				r = null;
				break;
			}else{
				BSONTimestamp newTs = (BSONTimestamp) tmp.get("cp");
				if(r == null){
					r = newTs;
				}else{
					r = minTimestamp(r,newTs);
				}
			}
		}
		
//		if(o == null){
//			//查询不到，取op log 最后的建表时间
//			DataSource sds = tableConfig.getSourceDataSource();
//			DBObject query = QueryBuilder.start("op").is("c").and("ns").is(sds.getDbName()+".$cmd").and("o").is(new BasicDBObject("create",sds.getTableName())).get();
//			DBObject orderBy =  new BasicDBObject("$natural", -1);
//			o = sourceDBC.findOne(query, null, orderBy );
//			if(o != null){
//			  r = (BSONTimestamp) o.get("ts");
//			}
//		}else{
//			r = (BSONTimestamp) o.get("cp");
//		}
		
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