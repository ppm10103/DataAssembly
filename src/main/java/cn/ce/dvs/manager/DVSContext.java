package cn.ce.dvs.manager;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.bson.types.BSONTimestamp;

import cn.ce.dvs.config.DataSource;
import cn.ce.dvs.manager.Action;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * dvs上下文
 * @author Administrator
 *
 */
public class DVSContext {
	
	public static volatile boolean run = false;
	//mongo客户端缓存,key为数据源实例
	public static ConcurrentMap<String, MongoClient> mongos = new ConcurrentHashMap<String, MongoClient>();
	public static ConcurrentMap<String, CommonsHttpSolrServer> solrs = new ConcurrentHashMap<String, CommonsHttpSolrServer>();
	
	public static BlockingQueue<Action>[] writeQueueList = null;
	public static ConcurrentMap<String, BSONTimestamp> positions = null;
	public static AtomicLong writeCount = new AtomicLong(0);
	
	public static DBCollection getDBC(DataSource ds,Map<String, MongoClient> mongos){
		MongoClient mc = mongos.get(ds.getDataSourceId());
		return mc.getDB(ds.getDbName()).getCollection(ds.getTableName());
	}
	public static DB getDB(DataSource ds,Map<String, MongoClient> mongos){
		MongoClient mc = mongos.get(ds.getDataSourceId());
		return mc.getDB(ds.getDbName());
	}
	
	public static DBCollection getDBC(String dsid,String dbName,String dbcName,Map<String, MongoClient> mongos){
		MongoClient mc = mongos.get(dsid);
		return mc.getDB(dbName).getCollection(dbcName);
	}
	
	public static CommonsHttpSolrServer getSolrServer(DataSource ds,Map<String, CommonsHttpSolrServer> solrs){
		return solrs.get(ds.getDataSourceId());
	}
	
	public static SolrDocument findById(CommonsHttpSolrServer solr,String keyName,String id) throws SolrServerException{
		String q = keyName+":\""+ id+"\"";
		ModifiableSolrParams solrParams = new ModifiableSolrParams();
		solrParams.set("q", q);
		solrParams.set("start", 0);
		solrParams.set("rows", 1);
		
		QueryResponse resp = solr.query(solrParams, METHOD.POST);
		SolrDocumentList rList = resp.getResults();
		if(rList != null && rList.size() > 0){
			return rList.get(0);
		}else{
			return null;
		}
	}
	
}
