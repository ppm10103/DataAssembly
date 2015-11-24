package cn.ce.dvs.manager;

import org.bson.types.BSONTimestamp;

import cn.ce.dvs.config.SubtaskConfig;

import com.mongodb.DBObject;

/**
 * 
 * 一次操作
 * @author Administrator
 *
 */
public class Action {
	//操作id
	private String id;
	//操作位置key
	private String positionKey;
	//配置信息
	private SubtaskConfig tableConfig;
	//hash值决定其属于哪个写队列
	private int hash;
	//操作时间位置
	private BSONTimestamp postion;
	//源dbo
	private DBObject sourceDBO;
	//目标dbo
	private DBObject targetDBO;
	//操作类型
	private OPType opType;
	
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	public DBObject getSourceDBO() {
		return sourceDBO;
	}
	public void setSourceDBO(DBObject sourceDBO) {
		this.sourceDBO = sourceDBO;
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
	public DBObject getTargetDBO() {
		return targetDBO;
	}
	public void setTargetDBO(DBObject targetDBO) {
		this.targetDBO = targetDBO;
	}
	public OPType getOpType() {
		return opType;
	}
	public void setOpType(OPType opType) {
		this.opType = opType;
	}
}
