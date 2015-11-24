package cn.ce.dvs.config;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;


@XmlRootElement(name = "dataSourceType")
@XmlAccessorType(value = XmlAccessType.FIELD)
public enum DataSourceType {
	mongo("MONGODB","mongodb"),mysql("MYSQL","mysql"),solr("SOLR","solr");
	private String key;
	private String description;
	
	private DataSourceType(String key,String description){
		this.key = key;
		this.description=description;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
	}
}
