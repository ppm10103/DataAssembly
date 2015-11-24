package cn.ce.dvs.manager;

import static cn.ce.dvs.manager.DVSContext.mongos;
import static cn.ce.dvs.manager.DVSContext.positions;
import static cn.ce.dvs.manager.DVSContext.run;
import static cn.ce.dvs.manager.DVSContext.getDBC;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.BSONTimestamp;

import cn.ce.dvs.config.DataSource;
import cn.ce.dvs.config.SubtaskConfig;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * 写检查点线程,子任务数*5000条或者两秒写一次检查点
 * @author Administrator
 *
 */
public class State implements Callable{
	
	public static final Long BEGIN_NUM = 0L;
	private static Log log = LogFactory.getLog(State.class);
	
	private List<SubtaskConfig> subtaskConfigList;

	public State(List<SubtaskConfig> subtaskConfigList) {
		this.subtaskConfigList = subtaskConfigList;
	}
	
	private void writeCheckPoint(DataSource ds,String subtaskConfigId,BSONTimestamp checkPoint){
		DBCollection dbc = getDBC(ds,mongos);
		DBObject jo = new BasicDBObject();
		
		jo.put("_id", subtaskConfigId);
		jo.put("cp", checkPoint);
		dbc.save(jo);
	}

	public Object call() throws Exception{
		
		Map<String, Long> subtaskCountContext = new HashMap<String, Long> ();
		Map<String, BSONTimestamp> subtaskCurrentCheckPointContext = new HashMap<String, BSONTimestamp> ();
		
		for (SubtaskConfig subtaskConfig : subtaskConfigList) {
			subtaskCountContext.put(subtaskConfig.getId(), BEGIN_NUM);
		}
		
		try {
			long ts = 0L;
			int c = 0;
			
			int s = subtaskConfigList.size() * 5000;
			while(run){
				if(positions.isEmpty()){
					TimeUnit.MILLISECONDS.sleep(2000);
				}else{
					for (SubtaskConfig subtaskConfig : subtaskConfigList) {
						
						String subtaskConfigId = subtaskConfig.getId();
						Long currentPositionCount = subtaskCountContext.get(subtaskConfigId);
						String currentPositionKey = subtaskConfigId+"#"+currentPositionCount;
						BSONTimestamp currentPosition = positions.get(currentPositionKey);
						
						if(currentPosition != null){
							positions.remove(currentPositionKey);
							subtaskCurrentCheckPointContext.put(subtaskConfigId, currentPosition);
							
							currentPositionCount++;
							long nextPositionCount = currentPositionCount;
							subtaskCountContext.put(subtaskConfigId, nextPositionCount);
							c++;
						}
					}
					
				}
				//写检查点
				if(c - s >= 0 || System.currentTimeMillis() - ts >= 2000L){
//					System.out.println(c + "_check:"+(System.currentTimeMillis() - ts));
					for (SubtaskConfig subtaskConfig : subtaskConfigList) {
						String subtaskConfigId = subtaskConfig.getId();
						DataSource ds = subtaskConfig.getCheckPointDataSource();
						BSONTimestamp p = subtaskCurrentCheckPointContext.get(subtaskConfigId);
						if(p != null){
							writeCheckPoint(ds,subtaskConfigId,p);
							subtaskCurrentCheckPointContext.remove(subtaskConfigId);
						}
					}
					ts = System.currentTimeMillis();
					if(c - s >= 0){
						c = c-s;
					}
				}
			}
		} catch (InterruptedException e) {
//			e.printStackTrace();
			Thread.interrupted();
		}
		return null;
	}
}