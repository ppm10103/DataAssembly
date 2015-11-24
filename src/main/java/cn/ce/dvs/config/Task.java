package cn.ce.dvs.config;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;


@XmlRootElement(name = "task")
@XmlAccessorType(value = XmlAccessType.FIELD)
public class Task {

	@XmlElementRef(name = "subtaskList")
	private List<SubtaskConfig> list = new ArrayList<SubtaskConfig>();

	public List<SubtaskConfig> getList() {
		return list;
	}

	public void setList(List<SubtaskConfig> list) {
		this.list = list;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
	}

}
