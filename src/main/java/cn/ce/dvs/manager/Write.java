package cn.ce.dvs.manager;

import static cn.ce.dvs.manager.DVSContext.findById;
import static cn.ce.dvs.manager.DVSContext.getDBC;
import static cn.ce.dvs.manager.DVSContext.getSolrServer;
import static cn.ce.dvs.manager.DVSContext.mongos;
import static cn.ce.dvs.manager.DVSContext.positions;
import static cn.ce.dvs.manager.DVSContext.run;
import static cn.ce.dvs.manager.DVSContext.solrs;
import static cn.ce.dvs.manager.DVSContext.writeCount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import cn.ce.dvs.config.DataSourceType;
import cn.ce.dvs.config.SubtaskConfig;

import com.alibaba.fastjson.JSON;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;

/**
 * 写线程
 * @author Administrator
 *
 */
public class Write implements Callable{
	
			private static Log log = LogFactory.getLog(Write.class);
	
			private BlockingQueue<Action> queue;
			private long count;
			
			public Write(BlockingQueue<Action> queue){
				this.queue = queue;
			}
			
			private DBObject toTargetDBO(Map<String, String> fieldMap,DBObject oldDBO){
				
				DBObject newDBO = new BasicDBObject();
				
				if(fieldMap == null || fieldMap.size() <= 0){
					return newDBO;
				}
				for (Entry<String, String> entry : fieldMap.entrySet()) {
					Object val = null;
					String sourceField = entry.getKey();
					if(sourceField.startsWith("$")){
						//简单实现length指令,只有String类型支持此参数
						//格式如下:$custNameCn.length
						String[] sub = sourceField.substring(1).split("\\.",2);
						String field = sub[0];
					    if("length".equals(sub[1])){
					    	Object sourceFieldValue = oldDBO.get(field);
					    	if(sourceFieldValue != null && sourceFieldValue instanceof String){
					    		val = sourceFieldValue.toString().length();
					    	}
					    }else{
					    	log.error("未知配置 :" + sourceField);
					    }
					}else{
						val = oldDBO.get(sourceField);
					}
					
					String[] targetFields = entry.getValue().split(",");
					for (String field : targetFields) {
						if(field == null){
							continue;
						}
						newDBO.put(field, val);
					}
				}
				
				return newDBO;
			}
			
			private DBObject preprocessDBObject(DBObject dbo) {
				return dbo;
			}
			
			private void doActionByMongo(Action action){
//			try {
				SubtaskConfig tc = action.getTableConfig();
				DBCollection dbc = getDBC(tc.getTargetDataSource(),mongos);
				String sourceMasterKey = tc.getSourceMasterKey();
				String targetMasterKey = tc.getTargetMasterKey();
				DBObject sourceDBO = action.getSourceDBO();
				Object targetKeyValue = sourceDBO.get(sourceMasterKey);
				Map<String, String> fieldMap = tc.getFieldMap();
				
				DBObject targetDBO = preprocessDBObject(sourceDBO);
				targetDBO = toTargetDBO(fieldMap, targetDBO);
				targetDBO.put(targetMasterKey, targetKeyValue);
				
				action.setTargetDBO(targetDBO);
				
				OPType opType = action.getOpType();
				
				switch (tc.getSynType()) {
				case f://一对一
					if(opType.equals(OPType.update) || opType.equals(OPType.insert)){
						dbc.update(new BasicDBObject(targetMasterKey, targetKeyValue),new BasicDBObject("$set",action.getTargetDBO()),true,false);
					}else if(opType.equals(OPType.delete)){
						if(tc.isMaster()){
							dbc.remove(new BasicDBObject(targetMasterKey, targetKeyValue));
						}else{
							DBObject dbo = action.getTargetDBO();
							dbo.removeField(targetMasterKey);
							for(String key : dbo.keySet()){
								dbo.put(key, null);
							}
							
							dbc.update(new BasicDBObject(targetMasterKey, targetKeyValue),new BasicDBObject("$set",action.getTargetDBO()),false,false);
						}
					}
					break;
				case c://单表copy
					if(opType.equals(OPType.insert)){
						try {
							dbc.insert(action.getTargetDBO());
						} catch (DuplicateKeyException e) {
							log.warn(e.getMessage(), e);
						}
					}else if(opType.equals(OPType.update)){
						
						dbc.update(new BasicDBObject(targetMasterKey, targetKeyValue),new BasicDBObject("$set",action.getTargetDBO()),true,false);
					}else if(opType.equals(OPType.delete)){
						
						dbc.remove(new BasicDBObject(targetMasterKey, targetKeyValue));
					}
					break;
				case fs://1对多
//					long s = System.currentTimeMillis();
//					if(opType.equals(OPType.insert)){
//						if(tc.isMaster()){
//							dbc.update(new BasicDBObject(targetMasterKey, targetKeyValue),new BasicDBObject("$set",action.getTargetDBO()),true,false);
//						}else{
//							DBObject updateDBO = new BasicDBObject("$set",new BasicDBObject(targetMasterKey,targetKeyValue));
//							updateDBO.put("$addToSet", new BasicDBObject(targetSlaveColName,action.getTargetDBO()));
//							dbc.update(new BasicDBObject(targetMasterKey,targetKeyValue), updateDBO ,true,false);
//						}
//						
////						System.out.println("w:"+ (System.currentTimeMillis() - s));
//						return;
//					}
					
					if(opType.equals(OPType.insert) || opType.equals(OPType.update)){
						//insert update 都使用相同逻辑，先删后添加,防止重放时，数据重复插入
						if(tc.isMaster()){
							dbc.update(new BasicDBObject(targetMasterKey, targetKeyValue),new BasicDBObject("$set",action.getTargetDBO()),true,false);
						}else{
							String targetSlaveColName = tc.getTargetSlaveColName();
							targetDBO.removeField(targetMasterKey);//移除主表主键id
							String sourceSlaveKey = tc.getSourceSlaveKey();
							Object targetSlaveKeyValue = sourceDBO.get(sourceSlaveKey);
							targetDBO.put(tc.getTargetSlaveKey(), targetSlaveKeyValue); //放入从表主键id
							
							//先删后添加
							DBObject updateDBO = new BasicDBObject("$pull",new BasicDBObject(targetSlaveColName,new BasicDBObject(tc.getTargetSlaveKey(),targetSlaveKeyValue)));
							dbc.update(new BasicDBObject(targetMasterKey,targetKeyValue), updateDBO ,true,false);
							updateDBO = new BasicDBObject("$set",new BasicDBObject(targetMasterKey,targetKeyValue));
							updateDBO.put("$push", new BasicDBObject(targetSlaveColName,action.getTargetDBO()));
							dbc.update(new BasicDBObject(targetMasterKey,targetKeyValue), updateDBO ,true,false);
						}
					}else if(opType.equals(OPType.delete)){
						
						if(tc.isMaster()){
							dbc.remove(new BasicDBObject(targetMasterKey, targetKeyValue));
						}else{
							String targetSlaveColName = tc.getTargetSlaveColName();
							targetDBO.removeField(targetMasterKey);//移除主表主键id
							String sourceSlaveKey = tc.getSourceSlaveKey();
							Object targetSlaveKeyValue = sourceDBO.get(sourceSlaveKey);
							targetDBO.put(tc.getTargetSlaveKey(), targetSlaveKeyValue); //放入从表主键id
							
							DBObject updateDBO = new BasicDBObject("$pull",new BasicDBObject(targetSlaveColName,new BasicDBObject(tc.getTargetSlaveKey(),targetSlaveKeyValue)));
							dbc.update(new BasicDBObject(targetMasterKey,targetKeyValue), updateDBO ,false,false);
						}
					}
					break;
					
				default:
					log.error("no");
					break;
				}
//			} catch (DuplicateKeyException e) {
//				log.warn(e.getMessage(), e);
//			}
			}
			
			
			private int contains(List list,Object keyVal,Object keyName){
				for (int i = 0; i < list.size(); i++) {
					Object o = list.get(i);
					if(o == null){
						continue;
					}
					
					if(o instanceof String && ((String) o).startsWith("{")){
						try {
							Map tmp = JSON.parseObject(o.toString(), Map.class);
							if(keyVal.equals(tmp.get(keyName))){
								return i;
							}
						} catch (Exception e) {
							log.error("json 转换失败: "+ o);
						}
					}else{
						if(keyVal.equals(o)){
							return i;
						}
					}
				}
				return -1;
			}
			
			
			private void doActionBySolr(Action action) throws SolrServerException, IOException{

				SubtaskConfig tc = action.getTableConfig();
				CommonsHttpSolrServer solr = getSolrServer(tc.getTargetDataSource(), solrs);
				
				String sourceMasterKey = tc.getSourceMasterKey();
				String targetMasterKey = tc.getTargetMasterKey();
				DBObject sourceDBO = action.getSourceDBO();
				Object targetKeyValue = sourceDBO.get(sourceMasterKey);
				Map<String, String> fieldMap = tc.getFieldMap();
				
				DBObject targetDBO = preprocessDBObject(sourceDBO);
				targetDBO = toTargetDBO(fieldMap, targetDBO);
				targetDBO.put(targetMasterKey, targetKeyValue);
				action.setTargetDBO(targetDBO);
				
				OPType opType = action.getOpType();
				
				switch (tc.getSynType()) {
				case f:
					if(opType.equals(OPType.update) || opType.equals(OPType.insert)){
						
						SolrDocument doc = findById(solr, tc.getTargetMasterKey(), targetKeyValue.toString());
						
						SolrInputDocument inputDoc = new SolrInputDocument();
						if(doc != null){
							for (String key : doc.keySet()) {//原solr数据
								inputDoc.setField(key, doc.get(key));
							}
						}
						
						DBObject dbo = action.getTargetDBO();
						Map<?, ?> map = dbo.toMap();
						
						for (Entry<?, ?> entry : map.entrySet()) {//新更改数据，覆盖原数据
							inputDoc.setField(entry.getKey().toString(), entry.getValue());
						}
						
						UpdateRequest req = new UpdateRequest();
						req.setParam("commit", "true");
						req.add(inputDoc);
						req.process(solr);
						
					}else if(opType.equals(OPType.delete)){
						if(tc.isMaster()){
							UpdateRequest delete =  new UpdateRequest().deleteById(targetKeyValue.toString());
							delete.setParam("commit", "true");
							delete.process(solr);
							
						}else{
							SolrDocument doc = findById(solr, tc.getTargetMasterKey(), targetKeyValue.toString());
							if(doc != null){
								SolrInputDocument inputDoc = new SolrInputDocument();
								for (String key : doc.keySet()) {//原solr数据
									inputDoc.setField(key, doc.get(key));
								}
								
								DBObject dbo = action.getTargetDBO();
								dbo.removeField(targetMasterKey);
								for(String key : dbo.keySet()){
									inputDoc.setField(key, null);
								}
								
								UpdateRequest req = new UpdateRequest();
								req.setParam("commit", "true");
								req.add(inputDoc);
								req.process(solr);
							}
						}
					}
					break;
				case c://单表copy
					if(opType.equals(OPType.update) || opType.equals(OPType.insert)){
						DBObject dbo = action.getTargetDBO();
						Map<?, ?> map = dbo.toMap();
						UpdateRequest req = new UpdateRequest();
						SolrInputDocument doc = new SolrInputDocument();
						for (Entry entry : map.entrySet()) {
							doc.setField(entry.getKey().toString(), entry.getValue());
						}
						req.add(doc);
						req.setCommitWithin(1000);
						req.process(solr);
					}else if(opType.equals(OPType.delete)){
						UpdateRequest delete =  new UpdateRequest().deleteById(targetKeyValue.toString());
							delete.setParam("commit", "true");
							delete.process(solr);
					}
					break;
				case fs:
					if(opType.equals(OPType.update) || opType.equals(OPType.insert)){
						
						SolrDocument doc = findById(solr, tc.getTargetMasterKey(), targetKeyValue.toString());
						
						if(tc.isMaster()){
							SolrInputDocument inputDoc = new SolrInputDocument();
							
							if(doc != null){
								for (String key : doc.keySet()) {
									inputDoc.setField(key, doc.get(key));
								}
							}
							
							DBObject dbo = action.getTargetDBO();
							Map<? , ?> map = dbo.toMap();
							for (Entry entry : map.entrySet()) {
								inputDoc.setField(entry.getKey().toString(), entry.getValue());
							}
							
							UpdateRequest req = new UpdateRequest();
							req.setParam("commit", "true");
							req.add(inputDoc);
							req.process(solr);
						}else{
							String targetSlaveColName = tc.getTargetSlaveColName();
							targetDBO.removeField(targetMasterKey);//移除主表主键
							String sourceSlaveKey = tc.getSourceSlaveKey();
							Object targetSlaveKeyValue = sourceDBO.get(sourceSlaveKey);
							targetDBO.put(tc.getTargetSlaveKey(), targetSlaveKeyValue); //放入从表主键
							
							SolrInputDocument inputDoc = new SolrInputDocument();
							List valList = null;
							if(doc != null){
								for (String key : doc.keySet()) {
									inputDoc.setField(key, doc.get(key));
								}
								Object val = doc.getFieldValue(targetSlaveColName);
								
								if(val != null){
									if(val instanceof List){
										valList = (List)val;
										int index = contains(valList, targetSlaveKeyValue, tc.getTargetSlaveKey());
										if(index != -1){//移除旧数据
											valList.remove(index);
										}
									} else {
										log.error("目标表的从表子集必须是list类型 : "+doc);
										return;
									}
								}else{
									valList = new ArrayList();
								}
								
							}else{
								valList = new ArrayList();
								inputDoc.setField(targetMasterKey, targetKeyValue);
							}
							
							if(fieldMap == null || fieldMap.size() <= 0){
								valList.add(targetSlaveKeyValue);
							}else{
								String json = null;
								try {
									json = JSON.toJSONString(targetDBO.toMap());
								} catch (Exception e) {
									log.error("targetDBO 转换失败:"+ targetDBO,e);
									return;
								}
								valList.add(json);
							}
							
							inputDoc.setField(targetSlaveColName, valList);
							
							UpdateRequest req = new UpdateRequest();
							req.setParam("commit", "true");
							req.add(inputDoc);
							req.process(solr);
						}
						
					}else if(opType.equals(OPType.delete)){
						if(tc.isMaster()){
							UpdateRequest delete =  new UpdateRequest().deleteById(targetKeyValue.toString());
							delete.setParam("commit", "true");
							delete.process(solr);
							
						}else{
							
							SolrDocument doc = findById(solr, tc.getTargetMasterKey(), targetKeyValue.toString());
							if(doc != null){
								SolrInputDocument inputDoc = new SolrInputDocument();
								for (String key : doc.keySet()) {
									inputDoc.setField(key, doc.get(key));
								}
								
								String targetSlaveColName = tc.getTargetSlaveColName();
								Object targetSlaveKeyValue = sourceDBO.get(tc.getSourceSlaveKey());
								
								Object slaveColVal = doc.getFieldValue(targetSlaveColName);
								if(slaveColVal != null){
									List valList = null;
									if(slaveColVal instanceof List){
										valList = (List)slaveColVal;
										int index = contains(valList, targetSlaveKeyValue, tc.getTargetSlaveKey());
										if(index != -1){//移除旧数据
											valList.remove(index);
										}
										
										inputDoc.setField(targetSlaveColName, valList);
										
										UpdateRequest req = new UpdateRequest();
										req.setParam("commit", "true");
										req.add(inputDoc);
										req.process(solr);
										
									} else {
										log.error("目标表的从表子集必须是list类型 : "+doc);
										return;
									}
								}
								
							}
						}
					}
					break;
				default:
					log.error("no");
					break;
				}
			
				
			}
			
			private void doAction(Action action) throws SolrServerException, IOException{
				DataSourceType key = action.getTableConfig().getTargetDataSource().getDataSourceType();
				switch (key) {
				case mongo:
					doActionByMongo(action);
					break;
				case solr:
					doActionBySolr(action);
					break;
				default:
					log.error("Unsupported data source type :"+ key);
					break;
				}
			}
			

			@Override
			public Object call() throws Exception {
				long l = 0;
				try {
					while (run) {
						Action action = queue.take();
						if(action != null){
							count++;
							if(count % 1000 == 1 ){
								l = System.currentTimeMillis();
							}
							doAction(action);
							writeCount.getAndIncrement();
							positions.put(action.getPositionKey(), action.getPostion());
							
							if(count % 1000 == 0 ){
								log.info("Write:"+action.getTableConfig().getId()+"_"+ (System.currentTimeMillis() - l));
							}
						}
					}
				} catch (InterruptedException e) {
//					e.printStackTrace();
					Thread.interrupted();
				}
				return null;
			}
		}