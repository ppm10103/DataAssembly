package cn.ce.dvs.config;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

@XmlRootElement(name = "sataSource")
@XmlAccessorType(value = XmlAccessType.FIELD)
public class DataSource {
	//数据源id
	private String dataSourceId;
	//数据源库名
	private String dbName;
	//数据源表名
	private String tableName;
	//数据源连接字符串uri
	private String uri;
	//数据源类型
	private DataSourceType dataSourceType;

	public String getDataSourceId() {
		return dataSourceId;
	}

	public void setDataSourceId(String dataSourceId) {
		this.dataSourceId = dataSourceId;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public DataSource(String dataSourceId, String dbName, String tableName,
			String uri, DataSourceType dataSourceType) {
		super();
		this.dataSourceId = dataSourceId;
		this.dbName = dbName;
		this.tableName = tableName;
		this.uri = uri;
		this.dataSourceType = dataSourceType;
	}

	public DataSource() {
	}

	public DataSourceType getDataSourceType() {
		return dataSourceType;
	}

	public void setDataSourceType(DataSourceType dataSourceType) {
		this.dataSourceType = dataSourceType;
	}
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
	}}
