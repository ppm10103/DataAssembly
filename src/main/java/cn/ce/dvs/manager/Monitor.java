package cn.ce.dvs.manager;

import static cn.ce.dvs.manager.DVSContext.positions;
import static cn.ce.dvs.manager.DVSContext.run;
import static cn.ce.dvs.manager.DVSContext.writeQueueList;
import static cn.ce.dvs.manager.DVSContext.writeCount;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Monitor implements Callable{
	private static Log log = LogFactory.getLog(Monitor.class);

	public Object call() throws Exception{
		Map<Object, Object> map = new HashMap<Object, Object>();
		try {
			while (run) {
				for (int i = 0; i < writeQueueList.length; i++) {
					map.put(i, writeQueueList[i].size());
				}
				map.put("state", positions.size());
				map.put("writeCount", writeCount.get());
				//log.info("Monitor: "+map);
				TimeUnit.MILLISECONDS.sleep(2000);
			}
		} catch (InterruptedException e) {
//			e.printStackTrace();
			Thread.interrupted();
		}
		return null;
	}
	
}