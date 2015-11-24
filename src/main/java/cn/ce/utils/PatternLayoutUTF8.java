package cn.ce.utils;

import org.apache.log4j.PatternLayout;

/**
 * 
 * 
 * <ol>
 * <li>解决系统异常邮件乱码问题/li>
 * <li>editor:hanyouhui dateTime:2014年6月17日 上午10:40:42</li>
 * <li>email:hanyouhui@xinnet.com</li>
 * </ol>
 * 
 * 
 */
public class PatternLayoutUTF8 extends PatternLayout {

	@Override
	public String getContentType() {
		// TODO Auto-generated method stub
		return "text/plain;charset=UTF-8";
	}

}
