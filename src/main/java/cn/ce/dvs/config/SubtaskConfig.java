package cn.ce.dvs.config;

import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;


@XmlRootElement(name = "subtaskConfig")
@XmlAccessorType(value = XmlAccessType.FIELD)
public class SubtaskConfig {
	
	//子任务id
	private String id;
	//任务名称
	private String taskName;
	//源数据源
	private DataSource sourceDataSource;
	//目标数据源
	private DataSource targetDataSource;
	//检查点数据源
	private DataSource checkPointDataSource;

	//源表关联主键
	private String sourceMasterKey;
	//源表从表主键
	private String sourceSlaveKey;
	//目标关联主键
	private String targetMasterKey;
	//目标从表主键
	private String targetSlaveKey;
	//目标子集名称(一对多关系)
	private String targetSlaveColName;

	//同步类型(c("c","全表拷贝"),f("f","字段填充"),fs("fs","填充子集"))
	//c 单表全表拷贝
	//f 一对一关系的两表装配
	//fs 一对多关系的两表装配
	private SynType synType;
	//查表模式
	//noOp - 基于源表(中间表)查询
    //opLogFromMain - 基于源表的oplog日志查询(oplog.$main)
	//opLogFromRS - 基于源表的oplog日志查询(oplog.rs)
	private ReadMode readMode;
	
	//字段映射
	@XmlElementWrapper(name = "fieldMap")
	private Map<String, String> fieldMap;
	
	//是否是主表
	private boolean isMaster;
	//是否真删除(未实现)
	private boolean isDeleteByOpType;

	public boolean isMaster() {
		return isMaster;
	}

	public void setMaster(boolean isMaster) {
		this.isMaster = isMaster;
	}

	public SubtaskConfig() {
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public DataSource getSourceDataSource() {
		return sourceDataSource;
	}

	public void setSourceDataSource(DataSource sourceDataSource) {
		this.sourceDataSource = sourceDataSource;
	}

	public DataSource getTargetDataSource() {
		return targetDataSource;
	}

	public void setTargetDataSource(DataSource targetDataSource) {
		this.targetDataSource = targetDataSource;
	}

	public DataSource getCheckPointDataSource() {
		return checkPointDataSource;
	}

	public void setCheckPointDataSource(DataSource checkPointDataSource) {
		this.checkPointDataSource = checkPointDataSource;
	}

	public SynType getSynType() {
		return synType;
	}

	public void setSynType(SynType synType) {
		this.synType = synType;
	}

	public Map<String, String> getFieldMap() {
		return fieldMap;
	}

	public void setFieldMap(Map<String, String> fieldMap) {
		this.fieldMap = fieldMap;
	}

	public String getSourceMasterKey() {
		return sourceMasterKey;
	}

	public void setSourceMasterKey(String sourceMasterKey) {
		this.sourceMasterKey = sourceMasterKey;
	}

	public String getSourceSlaveKey() {
		return sourceSlaveKey;
	}

	public void setSourceSlaveKey(String sourceSlaveKey) {
		this.sourceSlaveKey = sourceSlaveKey;
	}

	public String getTargetMasterKey() {
		return targetMasterKey;
	}

	public void setTargetMasterKey(String targetMasterKey) {
		this.targetMasterKey = targetMasterKey;
	}

	public String getTargetSlaveKey() {
		return targetSlaveKey;
	}

	public void setTargetSlaveKey(String targetSlaveKey) {
		this.targetSlaveKey = targetSlaveKey;
	}

	public String getTargetSlaveColName() {
		return targetSlaveColName;
	}

	public void setTargetSlaveColName(String targetSlaveColName) {
		this.targetSlaveColName = targetSlaveColName;
	}

	public boolean isDeleteByOpType() {
		return isDeleteByOpType;
	}

	public void setDeleteByOpType(boolean isDeleteByOpType) {
		this.isDeleteByOpType = isDeleteByOpType;
	}

	public ReadMode getReadMode() {
		return readMode;
	}

	public void setReadMode(ReadMode readMode) {
		this.readMode = readMode;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
	}
}
