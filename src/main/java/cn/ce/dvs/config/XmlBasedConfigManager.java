package cn.ce.dvs.config;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import cn.ce.utils.io.JaxbContextUtil;


/**
 * 基于xml的配置管理
 * @author Administrator
 *
 */
public class XmlBasedConfigManager implements ConfigManager{
	private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
	//写线程池大小
	private int writePoolSize = AVAILABLE_PROCESSORS < 10 ? 10 : AVAILABLE_PROCESSORS*2;
	private static final String CONFIG_PATH = "config";
	//xml配置文件路径
	private String configPath = System.getProperty("user.dir") + File.separator+ CONFIG_PATH;
	
	
	public XmlBasedConfigManager(){
	}
	
	public XmlBasedConfigManager(String configPath){
		if(StringUtils.isNotBlank(configPath)){
			this.configPath = configPath;
		}
	}
	
	private List<File> checkAndGetFiles(String path){
		File directory = new File(path);
		
		if(!directory.exists() || !directory.isDirectory()){
			throw new RuntimeException("config path ["+ path+"] does not exist or is invalid directory");
		}
		List<File> list = (List<File>) FileUtils.listFiles(directory, new String[]{"xml"}, false);
		
		if(list == null || list.size() <= 0){
			throw new RuntimeException("path ["+ path+"] config files not found");
		}
		
		return list;
	}
	
	private <T> T xmlToBean(File file) throws Exception{
		return (T) JaxbContextUtil.unmarshal(Task.class,new FileInputStream(file),null);
	}
	
	public List<SubtaskConfig> loadConfig() {
		//检查并获取xml文件
		List<File> files = checkAndGetFiles(getConfigPath());
		List<SubtaskConfig> list = new ArrayList<SubtaskConfig>();
		try {
			for (File file : files) {
				Task t = xmlToBean(file);
				list.addAll(t.getList());
			}
		} catch (Exception e) {
			throw new RuntimeException("load config failed",e);
		}		
		return list;
	}
	
	public int getWritePoolSize() {
		return writePoolSize;
	}


	public void setWritePoolSize(int writePoolSize) {
		this.writePoolSize = writePoolSize;
	}

	public String getConfigPath() {
		return configPath;
	}

	public void setConfigPath(String configPath) {
		this.configPath = configPath;
	}
}
