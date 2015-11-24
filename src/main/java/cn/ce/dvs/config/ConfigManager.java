package cn.ce.dvs.config;

import java.util.List;

/**
 * 配置管理
 * @author Administrator
 *
 */
public interface ConfigManager {
	/**
	 * 加载配置
	 * @return 返回子任务列表
	 */
	public List<SubtaskConfig> loadConfig();
	
	/**
	 * 写线程池大小
	 * @return 
	 */
	public int getWritePoolSize();
	public void setWritePoolSize(int writePoolSize);
}
