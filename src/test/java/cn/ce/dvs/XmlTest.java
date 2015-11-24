package cn.ce.dvs;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.junit.Test;

import cn.ce.dvs.config.DataSource;
import cn.ce.dvs.config.DataSourceType;
import cn.ce.dvs.config.ReadMode;
import cn.ce.dvs.config.SubtaskConfig;
import cn.ce.dvs.config.SynType;
import cn.ce.dvs.config.Task;
import cn.ce.dvs.config.XmlBasedConfigManager;
import cn.ce.utils.Utils;
import cn.ce.utils.io.JaxbContextUtil;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
public class XmlTest {
//		@Test
//		public void test() throws Exception {
//			
//			XmlBasedConfigManager x = new XmlBasedConfigManager(null);
//			
//			List<SubtaskConfig> list = new ArrayList<SubtaskConfig>();
//			SubtaskConfig e = new SubtaskConfig();
//			e.setTaskName("task1");
//			
//			e.setSourceDataSource(new DataSource("mongo1","DVS_MIDDLE_DB","BMA_DEV_LIU_YING_TEST","mongodb://172.23.164.57:27017/?safe=true",DataSourceType.mongo));
//			e.setTargetDataSource(new DataSource("mongo1","tTestDB","tTestTable","mongodb://172.23.164.57:27017/?safe=true",DataSourceType.mongo));
//			e.setCheckPointDataSource(new DataSource("mongo1","checkPointDB","checkPointTable","mongodb://172.23.164.57:27017/?safe=true",DataSourceType.mongo));
//			e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
//			e.setSynType(SynType.f);
//			e.setMaster(true);
//			e.setSourceMasterKey("ID");
//			e.setTargetMasterKey("id");
//			Map<String, String> fieldMap = new HashMap<String, String>();
////			fieldMap.put("CUSTID", "custId");
//			fieldMap.put("NAME", "name");
//			fieldMap.put("SOURCE", "source");
//			e.setFieldMap(fieldMap );
//			
//			list.add(e);
//			e = new SubtaskConfig();
//			e.setTaskName("task2");
//			e.setSourceDataSource(new DataSource("mongo1","DVS_MIDDLE_DB","BMA_DEV_LIU_YING_TEST_SLAVE","mongodb://172.23.164.57:27017/?safe=true",DataSourceType.mongo));
//			e.setTargetDataSource(new DataSource("mongo1","tTestDB","tTestTable","mongodb://172.23.164.57:27017/?safe=true",DataSourceType.mongo));
//			e.setCheckPointDataSource(new DataSource("mongo1","checkPointDB","checkPointTable","mongodb://172.23.164.57:27017/?safe=true",DataSourceType.mongo));
//			e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
//			e.setSynType(SynType.fs);
//			e.setMaster(false);
//			e.setSourceMasterKey("PARENT_ID");
//			e.setTargetMasterKey("id");
//			
//			e.setSourceSlaveKey("ID");
//			e.setTargetSlaveKey("child_id");
//			e.setTargetSlaveColName("sub");
//			fieldMap = new HashMap<String, String>();
//			fieldMap.put("DESCRIPTION", "description");
//			fieldMap.put("TYPE", "type");
//			e.setFieldMap(fieldMap );
//			
//			list.add(e);
//
//			Task t = new Task();
//			t.setList(list);
//			JaxbContextUtil.marshall(t, x.getConfigPath() + "/t.xml");
//
//			Task tr = JaxbContextUtil.unmarshal(Task.class,
//					new FileInputStream(new File(x.getConfigPath() + "/t.xml")),
//					null);
//			System.out.println(tr);
//		
//		}
	
	@Test
	public void test_solr_c() throws Exception{
		String sip = "172.23.166.23";
		String tip = "10.12.23.102";
		
		String suri = "mongodb://"+sip+":27020/?safe=true";
		String turi = "http://"+tip+":8081/solr/cust";
		String curi = "mongodb://"+sip+":27020/?safe=true";
		
		XmlBasedConfigManager x = new XmlBasedConfigManager(null);
		List<SubtaskConfig> list = new ArrayList<SubtaskConfig>();
		SubtaskConfig e = new SubtaskConfig();
		e.setTaskName("task0");
		
		e.setSourceDataSource(new DataSource(suri,"Mytest2","cust",suri,DataSourceType.mongo));
		e.setTargetDataSource(new DataSource(turi,null,null,turi,DataSourceType.solr));
		e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev_test_1","checkPointTable_1",curi,DataSourceType.mongo));
		e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
		e.setSynType(SynType.c);
		e.setMaster(true);
		e.setSourceMasterKey("custId");
		e.setTargetMasterKey("id");
		e.setReadMode(ReadMode.opLogFromRS);
		
		Map<String, String> fieldMap = new HashMap<String, String>();
		
		fieldMap.put("custNameCn","custName");
		fieldMap.put("custCreatTime","createTime");
		fieldMap.put("isOrder","ISORDER");
//		fieldMap.put("buyProduct","buyProduct");
		
		e.setFieldMap(fieldMap);
		
		list.add(e);
		
		Task t = new Task();
		t.setList(list);
		JaxbContextUtil.marshall(t, x.getConfigPath() + "/solr_test.xml");
		
		Task tr = JaxbContextUtil.unmarshal(Task.class,
				new FileInputStream(new File(x.getConfigPath() + "/solr_test.xml")),
				null);
		System.out.println(tr);
	}
	
	
	@Test
	public void test_solr_f() throws Exception{
		String sip = "172.23.166.23";
		String tip = "10.12.23.102";
		
		String suri = "mongodb://"+sip+":27020/?safe=true";
		String turi = "http://"+tip+":8081/solr/cust";
		String curi = "mongodb://"+sip+":27020/?safe=true";
		
		XmlBasedConfigManager x = new XmlBasedConfigManager(null);
		List<SubtaskConfig> list = new ArrayList<SubtaskConfig>();
		SubtaskConfig e = new SubtaskConfig();
		e.setTaskName("task0");
		
		e.setSourceDataSource(new DataSource(suri,"Mytest2","cust",suri,DataSourceType.mongo));
		e.setTargetDataSource(new DataSource(turi,null,null,turi,DataSourceType.solr));
		e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev_test_1","checkPointTable_1",curi,DataSourceType.mongo));
		e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
		e.setSynType(SynType.f);
		e.setMaster(true);
		e.setSourceMasterKey("custId");
		e.setTargetMasterKey("id");
		e.setReadMode(ReadMode.opLogFromRS);
		
		Map<String, String> fieldMap = new HashMap<String, String>();
		
		fieldMap.put("custNameCn","custName");
		fieldMap.put("custCreatTime","createTime");
		fieldMap.put("isOrder","ISORDER");
		e.setFieldMap(fieldMap);
		list.add(e);
		e = new SubtaskConfig();
		e.setTaskName("task0");
		
		e.setSourceDataSource(new DataSource(suri,"Mytest2","custInfo",suri,DataSourceType.mongo));
		e.setTargetDataSource(new DataSource(turi,null,null,turi,DataSourceType.solr));
		e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev_test_1","checkPointTable_1",curi,DataSourceType.mongo));
		e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
		e.setSynType(SynType.f);
		e.setMaster(false);
		e.setSourceMasterKey("custId");
		e.setTargetMasterKey("id");
		e.setSourceSlaveKey("custId");
		e.setTargetSlaveKey("id");
		
		e.setReadMode(ReadMode.opLogFromRS);
		
		fieldMap = new HashMap<String, String>();
		fieldMap.put("email","email");
		e.setFieldMap(fieldMap);
		list.add(e);
		
		Task t = new Task();
		t.setList(list);
		JaxbContextUtil.marshall(t, x.getConfigPath() + "/solr_test.xml");
		
		Task tr = JaxbContextUtil.unmarshal(Task.class,
				new FileInputStream(new File(x.getConfigPath() + "/solr_test.xml")),
				null);
		System.out.println(tr);
	}
	
	@Test
	public void test_solr_fs() throws Exception{
		String sip = "172.23.166.23";
		String tip = "10.12.23.102";
		
		String suri = "mongodb://"+sip+":27020/?safe=true";
		String turi = "http://"+tip+":8081/solr/cust";
		String curi = "mongodb://"+sip+":27020/?safe=true";
		
		XmlBasedConfigManager x = new XmlBasedConfigManager(null);
		List<SubtaskConfig> list = new ArrayList<SubtaskConfig>();
		SubtaskConfig e = new SubtaskConfig();
		e.setTaskName("task1");
		
		e.setSourceDataSource(new DataSource(suri,"Mytest2","cust",suri,DataSourceType.mongo));
		e.setTargetDataSource(new DataSource(turi,null,null,turi,DataSourceType.solr));
		e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev_test_1","checkPointTable_1",curi,DataSourceType.mongo));
		e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
		e.setSynType(SynType.fs);
		e.setMaster(true);
		e.setSourceMasterKey("custId");
		e.setTargetMasterKey("id");
		e.setReadMode(ReadMode.opLogFromRS);
		
		Map<String, String> fieldMap = new HashMap<String, String>();
		
		fieldMap.put("custNameCn","custName,custNameAll");
		fieldMap.put("$custNameCn.length","custNameLen");
		fieldMap.put("custCreatTime","createTime");
		fieldMap.put("isOrder","ISORDER");
		e.setFieldMap(fieldMap);
		list.add(e);
		e = new SubtaskConfig();
		e.setTaskName("task1");
		
		e.setSourceDataSource(new DataSource(suri,"Mytest2","product",suri,DataSourceType.mongo));
		e.setTargetDataSource(new DataSource(turi,null,null,turi,DataSourceType.solr));
		e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev_test_1","checkPointTable_1",curi,DataSourceType.mongo));
		e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
		e.setSynType(SynType.fs);
		e.setMaster(false);
		e.setSourceMasterKey("custId");
		e.setTargetMasterKey("id");
		e.setSourceSlaveKey("ID");
		e.setTargetSlaveKey("id");
		e.setTargetSlaveColName("buyProduct");
		
		e.setReadMode(ReadMode.opLogFromRS);
		
		fieldMap = new HashMap<String, String>();
		fieldMap.put("name","name");
		e.setFieldMap(fieldMap);
		list.add(e);
		
		Task t = new Task();
		t.setList(list);
		JaxbContextUtil.marshall(t, x.getConfigPath() + "/solr_test2.xml");
		
		Task tr = JaxbContextUtil.unmarshal(Task.class,
				new FileInputStream(new File(x.getConfigPath() + "/solr_test2.xml")),
				null);
		System.out.println(tr);
	}
	
	
	
	
	@Test
	public void test0() throws Exception{
		String sip = "172.23.164.57";
		String tip = "172.23.164.57";
		
		String suri = "mongodb://"+sip+":27017/?safe=true";
		String turi = "mongodb://"+tip+":27017/?safe=true";
		String curi = "mongodb://"+tip+":27017/?safe=true";
		
		XmlBasedConfigManager x = new XmlBasedConfigManager(null);
		List<SubtaskConfig> list = new ArrayList<SubtaskConfig>();
		SubtaskConfig e = new SubtaskConfig();
		e.setTaskName("task0");
		
		e.setSourceDataSource(new DataSource(suri,"DVS_MIDDLE_DB","BMA_DEV_LIU_YING_TEST",suri,DataSourceType.mongo));
		e.setTargetDataSource(new DataSource(turi,"DVS_TEST_1","BMA_DEV_LIU_YING_1",turi,DataSourceType.mongo));
		
		e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev_test_1","checkPointTable_1",curi,DataSourceType.mongo));
		e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
		e.setSynType(SynType.fs);
		e.setMaster(true);
		e.setSourceMasterKey("ID");
		e.setTargetMasterKey("id");
		e.setReadMode(ReadMode.opLogFromMain);
		
		MongoClient mongo = new MongoClient (new MongoClientURI(suri));
		DBCollection c1 = mongo.getDB("DVS_MIDDLE_DB").getCollection("BMA_DEV_LIU_YING_TEST");
		DBObject one = c1.findOne();
		one.removeField("_id");
		one.removeField("dvs_server_ts");
		one.removeField("dvs_thread_code");
		one.removeField("dvs_mysql_op_type");
		one.removeField("dvs_client_rec");
		one.removeField("when");
		one.removeField("_class");
		one.removeField("ID");
		
		Map<String, String> fieldMap = new HashMap<String, String>();
		for (String key : one.keySet()) {
			fieldMap.put(key, key);
		}
		
		e.setFieldMap(fieldMap );
		
		list.add(e);
		
		e = new SubtaskConfig();
		e.setTaskName("task0");
		
		e.setSourceDataSource(new DataSource(suri,"DVS_MIDDLE_DB","BMA_DEV_LIU_YING_TEST_SLAVE",suri,DataSourceType.mongo));
		e.setTargetDataSource(new DataSource(turi,"DVS_TEST_1","BMA_DEV_LIU_YING_1",turi,DataSourceType.mongo));
		
		e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev_test_1","checkPointTable_1",curi,DataSourceType.mongo));
		e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
		e.setSynType(SynType.fs);
		e.setMaster(false);
		e.setSourceMasterKey("PARENT_ID");
		e.setTargetMasterKey("id");
		e.setSourceSlaveKey("ID");
		e.setTargetSlaveKey("cid");
		e.setTargetSlaveColName("sub_col");
		e.setReadMode(ReadMode.opLogFromMain);
		
		mongo = new MongoClient (new MongoClientURI(suri));
		c1 = mongo.getDB("DVS_MIDDLE_DB").getCollection("BMA_DEV_LIU_YING_TEST_SLAVE");
		one = c1.findOne();
		one.removeField("_id");
		one.removeField("dvs_server_ts");
		one.removeField("dvs_client_rec");
		one.removeField("dvs_thread_code");
		one.removeField("dvs_mysql_op_type");
		one.removeField("dvs_client_rec");
		one.removeField("when");
		one.removeField("_class");
		one.removeField("PARENT_ID");
		one.removeField("ID");
		
		fieldMap = new HashMap<String, String>();
		for (String key : one.keySet()) {
			fieldMap.put(key, key);
		}
		
		e.setFieldMap(fieldMap );
		list.add(e);
		//##################################################################################
		e = new SubtaskConfig();
		e.setTaskName("task1");
		
		e.setSourceDataSource(new DataSource(suri,"DVS_MIDDLE_DB","BMA_DEV_LIU_YING_TEST",suri,DataSourceType.mongo));
		e.setTargetDataSource(new DataSource(turi,"DVS_TEST_2","BMA_DEV_LIU_YING_2",turi,DataSourceType.mongo));
		
		e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev_test_1","checkPointTable_1",curi,DataSourceType.mongo));
		e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
		e.setSynType(SynType.c);
		e.setMaster(true);
		e.setSourceMasterKey("ID");
		e.setTargetMasterKey("id");
		e.setReadMode(ReadMode.opLogFromMain);
		
		mongo = new MongoClient (new MongoClientURI(suri));
		c1 = mongo.getDB("DVS_MIDDLE_DB").getCollection("BMA_DEV_LIU_YING_TEST");
		one = c1.findOne();
		one.removeField("_id");
		one.removeField("dvs_server_ts");
		one.removeField("dvs_thread_code");
		one.removeField("dvs_mysql_op_type");
		one.removeField("dvs_client_rec");
		one.removeField("when");
		one.removeField("_class");
		one.removeField("ID");
		
		fieldMap = new HashMap<String, String>();
		for (String key : one.keySet()) {
			fieldMap.put(key, key);
		}
		
		e.setFieldMap(fieldMap );
		
		list.add(e);
		
		
		
		Task t = new Task();
		t.setList(list);
		JaxbContextUtil.marshall(t, x.getConfigPath() + "/BMA_DEV_LIU_YING_test.xml");
		
		Task tr = JaxbContextUtil.unmarshal(Task.class,
				new FileInputStream(new File(x.getConfigPath() + "/BMA_DEV_LIU_YING_test.xml")),
				null);
		System.out.println(tr);
		
	}
	
	
	
	
//	@Test
//	public void test1() throws Exception{
//		
//		String suri = "mongodb://172.23.164.57:27017/?safe=true";
//		String turi = "mongodb://172.23.164.57:27017/?safe=true";
//		String curi = "mongodb://172.23.164.57:27017/?safe=true";
//		
//		XmlBasedConfigManager x = new XmlBasedConfigManager(null);
//		List<SubtaskConfig> list = new ArrayList<SubtaskConfig>();
//		SubtaskConfig e = new SubtaskConfig();
//		e.setTaskName("task1");
//		
//		e.setSourceDataSource(new DataSource(suri,"DVS_MIDDLE_DB","BMA_DEV_LIU_YING_TEST",suri,DataSourceType.mongo));
//		e.setTargetDataSource(new DataSource(turi,"DVS_TARGET_DB","BMA_DEV_LIU_YING",turi,DataSourceType.mongo));
//		
//		e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev_test","checkPointTable",curi,DataSourceType.mongo));
//		e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
//		e.setSynType(SynType.f);
//		e.setMaster(true);
//		e.setSourceMasterKey("ID");
//		e.setTargetMasterKey("id");
//		
//		MongoClient mongo = new MongoClient (new MongoClientURI(suri));
//		DBCollection c1 = mongo.getDB("DVS_MIDDLE_DB").getCollection("BMA_DEV_LIU_YING_TEST");
//		DBObject one = c1.findOne();
//		one.removeField("_id");
//		one.removeField("dvs_server_ts");
//		one.removeField("dvs_client_rec");
//		one.removeField("dvs_thread_code");
//		one.removeField("dvs_mysql_op_type");
//		one.removeField("dvs_client_rec");
//		one.removeField("when");
//		one.removeField("_class");
//		one.removeField("ID");
//		
//		Map<String, String> fieldMap = new HashMap<String, String>();
//		for (String key : one.keySet()) {
//			fieldMap.put(key, key);
//		}
//		
//		e.setFieldMap(fieldMap );
//		
//		list.add(e);
//		
//		e = new SubtaskConfig();
//		e.setTaskName("task1");
//		
//		e.setSourceDataSource(new DataSource(suri,"DVS_MIDDLE_DB","BMA_DEV_LIU_YING_TEST_SLAVE",suri,DataSourceType.mongo));
//		e.setTargetDataSource(new DataSource(turi,"dvs_db_dev","bma_dev_test",turi,DataSourceType.mongo));
//		
//		e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev_test","checkPointTable",curi,DataSourceType.mongo));
//		e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
//		e.setSynType(SynType.fs);
//		e.setMaster(false);
//		e.setSourceMasterKey("PARENT_ID");
//		e.setTargetMasterKey("id");
//		e.setSourceSlaveKey("ID");
//		e.setTargetSlaveKey("cid");
//		e.setTargetSlaveColName("sub_col");
//		
//		
//		mongo = new MongoClient (new MongoClientURI(suri));
//		c1 = mongo.getDB("DVS_MIDDLE_DB").getCollection("BMA_DEV_LIU_YING_TEST_SLAVE");
//		one = c1.findOne();
//		one.removeField("_id");
//		one.removeField("dvs_server_ts");
//		one.removeField("dvs_client_rec");
//		one.removeField("dvs_thread_code");
//		one.removeField("dvs_mysql_op_type");
//		one.removeField("dvs_client_rec");
//		one.removeField("when");
//		one.removeField("_class");
//		one.removeField("PARENT_ID");
//		one.removeField("ID");
//		
//		fieldMap = new HashMap<String, String>();
//		for (String key : one.keySet()) {
//			fieldMap.put(key, key);
//		}
//		
//		e.setFieldMap(fieldMap );
//		list.add(e);
//		
//		Task t = new Task();
//		t.setList(list);
//		JaxbContextUtil.marshall(t, x.getConfigPath() + "/bma_dev_test.xml");
//		
//		Task tr = JaxbContextUtil.unmarshal(Task.class,
//				new FileInputStream(new File(x.getConfigPath() + "/bma_dev_test.xml")),
//				null);
//		System.out.println(tr);
//		
//	}
		@Test
		public void test2() throws Exception{
			String suri = "mongodb://172.23.166.23:27020/?safe=true";
			String turi = "mongodb://172.23.166.23:27020/?safe=true";
			String curi = "mongodb://172.23.166.23:27020/?safe=true";
			
			XmlBasedConfigManager x = new XmlBasedConfigManager(null);
			List<SubtaskConfig> list = new ArrayList<SubtaskConfig>();
			SubtaskConfig e = new SubtaskConfig();
			e.setTaskName("task2");
			e.setSourceDataSource(new DataSource(suri,"DVS_MIDDLE_DB_DEV","CMA_DEV_CM_CUST",suri,DataSourceType.mongo));
			e.setTargetDataSource(new DataSource(turi,"DVS_TARGET_DB_TEST","CMA_DEV_CM_CUST",turi,DataSourceType.mongo));
			e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev","checkPointTable",curi,DataSourceType.mongo));
			e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
			e.setSynType(SynType.c);
			e.setMaster(true);
			e.setSourceMasterKey("custId");
			e.setTargetMasterKey("custId");
//			e.setReadMode(ReadMode.noOp);
//			e.getReadMode()
			
			MongoClient mongo = new MongoClient (new MongoClientURI(suri));
			DBCollection c1 = mongo.getDB("DVS_MIDDLE_DB_DEV").getCollection("CMA_DEV_CM_CUST");
			DBObject one = c1.findOne(new BasicDBObject("_id", new ObjectId("55682b64a91c7618456a219e")));
			one.removeField("_id");
			one.removeField("dvs_server_ts");
			one.removeField("dvs_client_rec");
			one.removeField("dvs_thread_code");
			one.removeField("dvs_mysql_op_type");
			one.removeField("when");
			one.removeField("_class");
			one.removeField("custId");
			
			Map<String, String> fieldMap = new HashMap<String, String>();
			for (String key : one.keySet()) {
				fieldMap.put(key, key);
			}
			
			e.setFieldMap(fieldMap );
			
			list.add(e);
			Task t = new Task();
			t.setList(list);
			JaxbContextUtil.marshall(t, x.getConfigPath() + "/CMA_DEV_CM_CUST.xml");

			Task tr = JaxbContextUtil.unmarshal(Task.class,
					new FileInputStream(new File(x.getConfigPath() + "/CMA_DEV_CM_CUST.xml")),
					null);
			System.out.println(tr);
		}
		
		
		@Test
		public void test3() throws Exception{
			String suri = "mongodb://172.23.166.23:27020/?safe=true";
			String turi = "mongodb://172.23.166.23:27020/?safe=true";
			String curi = "mongodb://172.23.166.23:27020/?safe=true";
			
			XmlBasedConfigManager x = new XmlBasedConfigManager(null);
			
			List<SubtaskConfig> list = new ArrayList<SubtaskConfig>();
			SubtaskConfig e = new SubtaskConfig();
			e.setTaskName("task3");
			e.setSourceDataSource(new DataSource(suri,"DVS_MIDDLE_DB_DEV","CMA_DEV_CM_LINK_MAN",suri,DataSourceType.mongo));
			e.setTargetDataSource(new DataSource(turi,"DVS_TARGET_DB_TEST","CMA_DEV_CM_LINK_MAN_CONTACTINFO",turi,DataSourceType.mongo));
			e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev","checkPointTable",curi,DataSourceType.mongo));
			e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
			e.setSynType(SynType.fs);
			e.setMaster(true);
			e.setSourceMasterKey("manId");
			e.setTargetMasterKey("manId");
//			e.setReadMode(ReadMode.opLogFromRS);
			
			MongoClient mongo = new MongoClient (new MongoClientURI(suri));
			DBCollection c1 = mongo.getDB("DVS_MIDDLE_DB_DEV").getCollection("CMA_DEV_CM_LINK_MAN");
			DBObject one = c1.findOne();
			one.removeField("_id");
			one.removeField("dvs_server_ts");
			one.removeField("dvs_client_rec");
			one.removeField("dvs_thread_code");
			one.removeField("dvs_client_rec");
			one.removeField("dvs_mysql_op_type");
			one.removeField("when");
			one.removeField("_class");
			one.removeField("manId");
			
			Map<String, String> fieldMap = new HashMap<String, String>();
			for (String key : one.keySet()) {
				fieldMap.put(key, key);
			}
			
			e.setFieldMap(fieldMap );
			
			list.add(e);
			e = new SubtaskConfig();
			e.setTaskName("task3");
			e.setSourceDataSource(new DataSource(suri,"DVS_MIDDLE_DB_DEV","CMA_DEV_CM_CONTACTINFO",suri,DataSourceType.mongo));
			e.setTargetDataSource(new DataSource(turi,"DVS_TARGET_DB_TEST","CMA_DEV_CM_LINK_MAN_CONTACTINFO",turi,DataSourceType.mongo));
			e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev","checkPointTable",curi,DataSourceType.mongo));
			e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
			e.setSynType(SynType.fs);
			e.setMaster(false);
			e.setSourceMasterKey("manId");
			e.setTargetMasterKey("manId");
			e.setSourceSlaveKey("contactId");
			e.setTargetSlaveKey("contactId");
			e.setTargetSlaveColName("sub_contactinfo");
//			e.setReadMode(ReadMode.opLogFromRS);
			
			mongo = new MongoClient (new MongoClientURI(suri));
			c1 = mongo.getDB("DVS_MIDDLE_DB_DEV").getCollection("CMA_DEV_CM_CONTACTINFO");
			one = c1.findOne();
			one.removeField("_id");
			one.removeField("dvs_server_ts");
			one.removeField("dvs_client_rec");
			one.removeField("dvs_thread_code");
			one.removeField("dvs_mysql_op_type");
			one.removeField("dvs_client_rec");
			one.removeField("when");
			one.removeField("_class");
			one.removeField("manId");
			one.removeField("contactId");
			
			fieldMap = new HashMap<String, String>();
			for (String key : one.keySet()) {
				fieldMap.put(key, key);
			}
			
			e.setFieldMap(fieldMap );
//			
			list.add(e);
			
			Task t = new Task();
			t.setList(list);
			JaxbContextUtil.marshall(t, x.getConfigPath() + "/CMA_DEV_CM_LINK_MAN_CONTACTINFO.xml");
			
			Task tr = JaxbContextUtil.unmarshal(Task.class,
					new FileInputStream(new File(x.getConfigPath() + "/CMA_DEV_CM_LINK_MAN_CONTACTINFO.xml")),
					null);
			System.out.println(tr);
			
		}
		
		@Test
		public void test4() throws Exception{
			String suri = "mongodb://172.23.164.57:27017/?safe=true";
			String turi = "mongodb://172.23.164.57:27017/?safe=true";
			String curi = "mongodb://172.23.164.57:27017/?safe=true";
			
			XmlBasedConfigManager x = new XmlBasedConfigManager(null);
			
			List<SubtaskConfig> list = new ArrayList<SubtaskConfig>();
			SubtaskConfig e = new SubtaskConfig();
			e.setTaskName("task4");
			
			e.setSourceDataSource(new DataSource(suri,"DVS_MIDDLE_DB","BMA_DEV_SALE_PRODUCT_INSTANCE",suri,DataSourceType.mongo));
			e.setTargetDataSource(new DataSource(turi,"DVS_TARGET_DB","BMA_DEV_SALE_PRODUCT_INSTANCE_SALE_PRODUCT_UNIT_INSTANCE",turi,DataSourceType.mongo));
			e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev","checkPointTable",curi,DataSourceType.mongo));
			e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
			e.setSynType(SynType.fs);
			e.setMaster(true);
			e.setSourceMasterKey("ID");
			e.setTargetMasterKey("ID");
			
			MongoClient mongo = new MongoClient (new MongoClientURI(suri));
			DBCollection c1 = mongo.getDB("DVS_MIDDLE_DB").getCollection("BMA_DEV_SALE_PRODUCT_INSTANCE");
			DBObject one = c1.findOne();
			one.removeField("_id");
			one.removeField("dvs_server_ts");
			one.removeField("dvs_client_rec");
			one.removeField("dvs_thread_code");
			one.removeField("dvs_mysql_op_type");
			one.removeField("dvs_client_rec");
			one.removeField("when");
			one.removeField("_class");
			one.removeField("ID");
			e.setReadMode(ReadMode.opLogFromMain);
			
			Map<String, String> fieldMap = new HashMap<String, String>();
			for (String key : one.keySet()) {
				fieldMap.put(key, key);
			}
			
			e.setFieldMap(fieldMap );
			
			list.add(e);
			e = new SubtaskConfig();
			e.setTaskName("task4");
			
			e.setSourceDataSource(new DataSource(suri,"DVS_MIDDLE_DB","BMA_DEV_SALE_PRODUCT_UNIT_INSTANCE",suri,DataSourceType.mongo));
			e.setTargetDataSource(new DataSource(turi,"DVS_TARGET_DB","BMA_DEV_SALE_PRODUCT_INSTANCE_SALE_PRODUCT_UNIT_INSTANCE",turi,DataSourceType.mongo));
			e.setCheckPointDataSource(new DataSource(curi,"checkPointDB_dev","checkPointTable",curi,DataSourceType.mongo));
			e.setId(Utils.concat("#", e.getTaskName(),e.getSourceDataSource().getDbName(),e.getSourceDataSource().getTableName()));
			e.setSynType(SynType.fs);
			e.setMaster(false);
			e.setSourceMasterKey("PRODUCT_INSTANCE_ID");
			e.setTargetMasterKey("ID");
			e.setSourceSlaveKey("ID");
			e.setTargetSlaveKey("unitInstanceId");
			e.setTargetSlaveColName("sub_unitInstance");
			e.setReadMode(ReadMode.opLogFromMain);
			
			
			mongo = new MongoClient (new MongoClientURI(suri));
			c1 = mongo.getDB("DVS_MIDDLE_DB").getCollection("BMA_DEV_SALE_PRODUCT_UNIT_INSTANCE");
			one = c1.findOne();
			one.removeField("_id");
			one.removeField("dvs_server_ts");
			one.removeField("dvs_client_rec");
			one.removeField("dvs_thread_code");
			one.removeField("dvs_mysql_op_type");
			one.removeField("dvs_client_rec");
			one.removeField("when");
			one.removeField("_class");
			one.removeField("PRODUCT_INSTANCE_ID");
			
			fieldMap = new HashMap<String, String>();
			for (String key : one.keySet()) {
				fieldMap.put(key, key);
			}
			
			e.setFieldMap(fieldMap );
//			
			list.add(e);
			
			Task t = new Task();
			t.setList(list);
			JaxbContextUtil.marshall(t, x.getConfigPath() + "/BMA_DEV_SALE_PRODUCT_INSTANCE_SALE_PRODUCT_UNIT_INSTANCE.xml");
			
			Task tr = JaxbContextUtil.unmarshal(Task.class,
					new FileInputStream(new File(x.getConfigPath() + "/BMA_DEV_SALE_PRODUCT_INSTANCE_SALE_PRODUCT_UNIT_INSTANCE.xml")),
					null);
			System.out.println(tr);
			
		}
}
