package cn.ce.utils;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;


public class Utils {
	private final static String PID_PATH = System.getProperty("user.dir") + File.separator+ "myPid.pid";
//	public static void checkString(String name,String val){
//		Assert.hasText(val, name+" must have text; it must not be null, empty, or blank");
//	}
	
	public static String concat(String delimiter,String ... val){
		StringBuilder sb = new StringBuilder();
		boolean isFirst = true;
		for (String string : val) {
			if(isFirst){
				sb.append(string);
				isFirst = false;
			}else{
				sb.append(delimiter).append(string);
			}
		}
		
		return sb.toString();
	}
	public static Set<String> getIpV4(){
		Set<String> ip = new HashSet<String>();
		try {
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			while (interfaces.hasMoreElements()) {
				NetworkInterface networkInterface = (NetworkInterface) interfaces.nextElement();
				if(networkInterface.isLoopback() || networkInterface.isVirtual()){
					continue;
				}
				if(networkInterface.getDisplayName().contains("VMware Virtual") || networkInterface.getDisplayName().contains("Wireless")){
					continue;
				}
				 List<InterfaceAddress> addresses = networkInterface.getInterfaceAddresses();
				for (InterfaceAddress interfaceAddress : addresses) {
					InetAddress inetAddress = interfaceAddress.getAddress();
					if(inetAddress != null && inetAddress instanceof Inet4Address && inetAddress.getHostAddress() != null){
						ip.add(inetAddress.getHostAddress());
					}
				}
			} 
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		if(ip.isEmpty()){
			InetAddress addr = null;
			try {
				addr = InetAddress.getLocalHost();
			} catch (UnknownHostException e1) {
			} 
			if(addr != null && addr instanceof Inet4Address && addr.getHostAddress() != null){
				ip.add(addr.getHostAddress());
			}
		}
		
		return ip;
	}
	
	    
	    public static int getPid() {
	    	int pid = -1;
            try {
                RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();  
                String name = runtime.getName(); // format: "pid@hostname"  
                pid = Integer.parseInt(name.substring(0, name.indexOf('@')));
            } catch (Throwable e) {
            	e.printStackTrace();
            }
	        return pid;  
	    }
	    
	    public static void writePid() throws Throwable{
	    	writePid(PID_PATH);
	    }
	    
	    public static void removePidFile(){
	    	removeFile(PID_PATH);
	    	
	    }
	    
	    public static void removeFile(String path){
	    	File file = new File(path);
			FileUtils.deleteQuietly(file );
	    }
	    
	    public static void writePid(String path) throws Throwable{
	    	int pid = getPid();
	    	FileUtils.write(new File(path), pid+"");
	    }
	    
	    
}
