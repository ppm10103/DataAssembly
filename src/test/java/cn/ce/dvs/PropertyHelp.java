package cn.ce.dvs;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.dvs.manager.Bootstrap;

public class PropertyHelp {
	
	    private Properties p = new Properties();
	    private PropertyHelp(){};
	    
	    public static PropertyHelp getInstance(String key,String defaultPath){
	    	PropertyHelp pu = new PropertyHelp();
	    	 try {
				String filePath = System.getProperty("user.dir") +File.separator +System.getProperty(key,defaultPath);
//				String filePath = System.getProperty("user.dir") +File.separator +System.getProperty("dvs.cfgPath","dvs.properties");
				InputStream in = new BufferedInputStream(new FileInputStream(filePath));
				pu.p.load(in);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
	    	return pu;
	    }

	    /**
	     * 根据key得到value的值
	     */
	    public String getValue(String key) {
	        return p.getProperty(key);
	    }
	    
	    public String getValue(String key,String defaultVal) {
	    	return getValue(key) == null ? defaultVal:getValue(key);
	    }
	    
	    public int getValue(String key,int defaultVal) {
	    	String val = getValue(key);
	    	if(val == null){
	    		return defaultVal;
	    	}
	    	return Integer.parseInt(val);
	    }
	
}
