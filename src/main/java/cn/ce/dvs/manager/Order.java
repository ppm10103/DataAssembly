package cn.ce.dvs.manager;

public enum Order {
	ASC(1,"正序"), DESC(-1,"倒序");
	private int key;
	private String description;

	private Order(int key,String description) {
		this.key = key;
		this.description = description;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}
