package cn.ce.dvs;

import org.bson.types.BSONTimestamp;

import cn.ce.dvs.config.SubtaskConfig;

import com.mongodb.DBObject;

/**
 * @author Administrator
 *
 */
public class Action {
	private String id;
	private String positionKey;
	private SubtaskConfig tableConfig;
	
	private int hash;
	private BSONTimestamp postion;
	
	private DBObject dbo;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	
	
	public DBObject getDbo() {
		return dbo;
	}
	public void setDbo(DBObject dbo) {
		this.dbo = dbo;
	}
	public BSONTimestamp getPostion() {
		return postion;
	}
	public void setPostion(BSONTimestamp postion) {
		this.postion = postion;
	}
	public int getHash() {
		return hash;
	}
	public void setHash(int hash) {
		this.hash = hash;
	}
	public String getPositionKey() {
		return positionKey;
	}
	public void setPositionKey(String positionKey) {
		this.positionKey = positionKey;
	}
	public SubtaskConfig getTableConfig() {
		return tableConfig;
	}
	public void setTableConfig(SubtaskConfig tableConfig) {
		this.tableConfig = tableConfig;
	}
	
}
