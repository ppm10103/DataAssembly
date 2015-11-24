package cn.ce.dvs.manager;

import static cn.ce.dvs.manager.DVSContext.getDBC;
import static cn.ce.dvs.manager.DVSContext.mongos;
import static cn.ce.dvs.manager.DVSContext.run;
import static cn.ce.dvs.manager.DVSContext.writeQueueList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.BSONTimestamp;

import cn.ce.dvs.config.SubtaskConfig;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * 查询线程
 * @author Administrator
 *
 */
public class Read implements Callable<Object>{
	
	private static Log log = LogFactory.getLog(Read.class);
	
	private long count = 0;
	private DBCollection sourceDBC = null;
	private DBCollection checkPointDBC= null;
	
	private BSONTimestamp start = new BSONTimestamp();
	private SubtaskConfig tableConfig = null;
	
	public Read(SubtaskConfig tableConfig) {
		this.tableConfig = tableConfig;
		this.sourceDBC = getDBC(tableConfig.getSourceDataSource(),mongos);
		this.checkPointDBC = getDBC(tableConfig.getCheckPointDataSource(),mongos);
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
//		if(hash == 0){
//			Random r = new Random(System.currentTimeMillis());
//			hash = r.nextInt();
//		}
		return  writeQueueList[Math.abs(hash) % writeQueueList.length];
	}
	
	private OPType getOPType(DBObject dbo){
		Object op = dbo.get("dvs_mysql_op_type") == null ?  dbo.get("dmlType"):dbo.get("dvs_mysql_op_type");
		if(op == null) return null;
		
		if(op.toString().equalsIgnoreCase(OPType.insert.getKey())){
			return OPType.insert;
		}
		if(op.toString().equalsIgnoreCase(OPType.update.getKey())){
			return OPType.update;
		}
		if(op.toString().equalsIgnoreCase(OPType.delete.getKey())){
			return OPType.delete;
		}
		
		return null;
	}
	
	private Action dboToAction(DBObject dbo){
		String sourceMasterKey = tableConfig.getSourceMasterKey();
		
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
		action.setPostion((BSONTimestamp) dbo.get("dvs_server_ts"));
		action.setSourceDBO(dbo);
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

	private  List<Action> getList(int limit){
		long tt = System.currentTimeMillis();
		DBObject ref = QueryBuilder.start("dvs_server_ts").greaterThan(this.start).get();
		log.info(tableConfig.getId() + " read begin: "+ ref);
		DBCursor c = sourceDBC.find(ref);
		DBObject orderBy = new BasicDBObject("dvs_server_ts",1);
		c.sort(orderBy);
		c.limit(limit);
		List<Action> list = new ArrayList<Action>();
		DBObject last = null;
		while (c.hasNext()) {
			last = (DBObject) c.next();
			Action action = dboToAction(last);
			if(action == null){
				continue;
			}
			list.add(action);
		}
		c.close();
		log.debug(tableConfig.getId() + " read last: "+ last);
		if(last != null){
			this.start = (BSONTimestamp) last.get("dvs_server_ts");
		}
		tt = System.currentTimeMillis() - tt;
		if(list.size() != 0){
			log.info(tableConfig.getId()+ " read:" + " elapsed time "+tt+" ms," + "size "+list.size());
		}
		return list;
	};
	
	private BSONTimestamp getPosition(){
		DBObject r = checkPointDBC.findOne(new BasicDBObject("_id",tableConfig.getId()));
		return r == null ? null : (BSONTimestamp) r.get("cp");
	}
	
	public Object call() throws Exception {
		BSONTimestamp p = getPosition();
		if(p == null){
			start = new BSONTimestamp();
		}else{
			start = p;
		}
		
		try {
			while (run) {
				//查询列表
				List<Action> l = getList(5000);
				//根据hash值放入队列
				for (Action data : l) {
					getQueue(data.getHash()).put(data);
				}
				
				int size = l.size();
				if(size == 0){
					TimeUnit.MILLISECONDS.sleep(2000);
				}
			}
		} catch (InterruptedException e) {
//			e.printStackTrace();
			Thread.interrupted();
		}
		return null;
	}
}