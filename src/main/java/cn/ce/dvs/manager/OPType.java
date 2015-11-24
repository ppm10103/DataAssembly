package cn.ce.dvs.manager;

public enum OPType {
	insert("INSERT","插入"),update("UPDATE","更新"),delete("DELETE","删除");
	private String key;
	private String description;
	
	private OPType(String key,String description){
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

}
