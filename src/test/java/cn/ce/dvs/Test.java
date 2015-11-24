package cn.ce.dvs;

import java.net.UnknownHostException;

import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

public class Test {
	
	public static void main(String[] args) {
	}
//	public static void main(String[] args) throws UnknownHostException {
//		MongoClient mongo = new MongoClient ("10.12.23.103");
//		DB c1 = mongo.getDB("checkPointDB");
//		c1.dropDatabase();
//		c1 = mongo.getDB("tTestDB");
//		c1.dropDatabase();
//		c1 = mongo.getDB("DVS_MIDDLE_DB");
//		c1.dropDatabase();
//		DBCollection c = mongo.getDB("DVS_MIDDLE_DB").getCollection("BMA_DEV_LIU_YING_TEST_SLAVE");
//		
//		long l = System.currentTimeMillis();
//		
//		
//		c.drop();
//		
//		
//		for (int i = 0; i < 1000; i++) {
//			DBObject arr = new BasicDBObject();
////			{
////				  "_id" : ObjectId("55385b0cb44cf9c815c7c275"),
////				  "dvs_server_ts" : {
////				    "$timestamp" : NumberLong("6140758199017406465")
////				  },
////				  "dvs_client_rec" : NumberLong("1429756163196"),
////				  "dvs_thread_code" : 97,
////				  "dvs_mysql_op_type" : "INSERT",
////				  "when" : NumberLong(1429756163),
////				  "ID" : "5",
////				  "NAME" : null,
////				  "PARENT_ID" : "1",
////				  "IS_COMMERCE" : null,
////				  "SOURCE" : null,
////				  "STATE" : null,
////				  "SORT" : null,
////				  "CODE" : null,
////				  "DESCRIPTION" : "23",
////				  "TYPE" : "232",
////				  "BUSINESS_TYPE" : 0,
////				  "IDC_AREA_TYPE" : null
////				}
//			
//			arr.put("dvs_server_ts", new BSONTimestamp());
//			arr.put("dvs_mysql_op_type", "INSERT");
//			arr.put("ID", i+"");
//			arr.put("PARENT_ID" , "1");
//			arr.put("DESCRIPTION" , "abc");
//			arr.put("TYPE" , "TYPE"+i);
//			c.insert(arr,WriteConcern.SAFE);
//		}
//		System.out.println(System.currentTimeMillis()-l);
//	}
}
