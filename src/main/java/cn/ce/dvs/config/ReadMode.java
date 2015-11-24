package cn.ce.dvs.config;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

@XmlRootElement(name = "readMode")
@XmlAccessorType(value = XmlAccessType.FIELD)
public enum ReadMode {
	
		noOp("noOp","查询原表","noOp"),opLogFromMain("opLogFromMain","从oplog.$main查询操作日志","oplog.$main"),opLogFromRS("opLogFromRS","从oplog.rs查询操作日志","oplog.rs");
		private String key;
		private String description;
		private String value;
		
		private ReadMode(String key,String description,String value){
			this.key = key;
			this.description=description;
			this.value = value;
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
		
		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this,
					ToStringStyle.MULTI_LINE_STYLE);
		}
}
