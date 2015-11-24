package cn.ce.dvs.config;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

@XmlRootElement(name = "synType")
@XmlAccessorType(value = XmlAccessType.FIELD)
public enum SynType {
		c("c","全表拷贝"),f("f","字段填充"),fs("fs","填充子集");
		private String key;
		private String description;
		
		private SynType(String key,String description){
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
