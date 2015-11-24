package cn.ce.dvs;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.bson.types.BSONTimestamp;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import com.mongodb.ServerAddress;


public class Main implements Observer{
//	public static long count1 = 0;
	public static int l = 10;
	public static BlockingQueue<Action>[] qs= new ArrayBlockingQueue[l];
	public static ConcurrentMap<Long, BSONTimestamp> positions = new ConcurrentHashMap<Long, BSONTimestamp>();
	
	public static long currentPositionCount = 1;
	public static long count = 0;
	public static volatile boolean run = true;
	public volatile BSONTimestamp position = new BSONTimestamp();
	
	public static ExecutorService pool = Executors.newCachedThreadPool();
	
	volatile List<Future<?>> list = new ArrayList<Future<?>>();
	
	public void run(){
		 Handler sh = new Handler();
         sh.addObserver(this);
         sh.handleSignal("INT");
         
         for (int i = 0; i < qs.length; i++) {
 			qs[i] = new ArrayBlockingQueue<Action>(2500);
 		}
         
        CompletionService<?> cs = new ExecutorCompletionService (pool);
        
        
        cs.submit(new Read());
 		
 		for (int i = 0; i < qs.length; i++) {
 			list.add(cs.submit(new Work(qs[i])));
 		}
 		cs.submit(new State());
// 		list.add(f);
 		cs.submit(new Monitor());
 		try {
//			for (int i = 0; i < list.size(); i++) {
//				Future<?> f = list.get(0); 
//				if(f.get() == null){
////					System.exit(0);
//					mongo.close();
//					pool.shutdownNow();
//				}
//			}
// 			Future<?> f = cs.take();
// 			if(f.get()==null){
// 				mongo.close();
// 				pool.shutdownNow();
// 			}
 			cs.take().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}catch (Throwable e) {
			e.printStackTrace();
		}finally{
			try {
				mongo.close();
			} catch (Exception e2) {
			}
			
			try {
				pool.shutdownNow();
			} catch (Exception e2) {
			}
		}
 		
// 		list.add(f);
	}
	private static MongoClient mongo;
	private static String host = ServerAddress.defaultHost();
	private static int port = ServerAddress.defaultPort();
//	private static MongoClient mongo2;
	private static void initConfig() {
		
	}
	public static void main(String[] args) {
		
		initConfig();
		if(args != null && args.length > 0){
			for (String string : args) {
				String[] temp = string.split(":",2);
				if("host".equals(temp[0])){
					host = temp[1];
				}else if("port".equals(temp[0])){
					port = Integer.parseInt(temp[1]);
				}
			}
		}
		try {
			mongo = new MongoClient (host,port);
//			mongo2 = new MongoClient ("10.12.23.103",port);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		new Main().run();
	}

	public BlockingQueue<Action> getQueue(int hash){
		if(hash == 0){
			Random r = new Random(System.currentTimeMillis());
			hash = r.nextInt();
		}
		return  qs[Math.abs(hash)%l];
	}
	
	class State implements Callable{
		
		public Object call() throws Exception{
			try {
				while(run){
					if(positions.isEmpty()){
						System.out.println("sleep" + 1000);
						TimeUnit.MILLISECONDS.sleep(1000);
					}else{
						BSONTimestamp p = positions.get(currentPositionCount);
						if(p != null){
							position = p;
							positions.remove(currentPositionCount);
							currentPositionCount++;
						}else{
							System.out.println("null sleep " + currentPositionCount);
							TimeUnit.MILLISECONDS.sleep(1000);
						}
					}
				}
			} catch (InterruptedException e) {
//				e.printStackTrace();
				Thread.interrupted();
			}
			return null;
		}
	}
	
	class Monitor implements Callable{

		public Object call() throws Exception{
			Map<Object, Object> map = new HashMap<Object, Object>();
			try {
				while (run) {
					for (int i = 0; i < qs.length; i++) {
						map.put(i, qs[i].size());
					}
					map.put("position", position);
					map.put("state", positions.size());
					System.err.println(map);
					TimeUnit.MILLISECONDS.sleep(2000);
				}
			} catch (InterruptedException e) {
				//e.printStackTrace();
				Thread.interrupted();
			}
			return null;
		}
		
	}
	
	class Read implements Callable{
		private DBCollection dbc = null;
		private BSONTimestamp start = new BSONTimestamp();
		
		
		private  List<Action> getList(int limit){
			long tt = System.currentTimeMillis();
			DBObject ref = QueryBuilder.start("dvs_server_ts").greaterThan(this.start).get();
			System.out.println(ref);
			DBCursor c = dbc.find(ref);
			DBObject orderBy = new BasicDBObject("dvs_server_ts",1);
			c.sort(orderBy);
//			c.batchSize(10000);
			c.limit(limit);
			List<Action> list = new ArrayList<Action>();
			DBObject last = null;
			while (c.hasNext()) {
				last = (DBObject) c.next();
				Action d = new Action();
				count++;
				//d.setCount(count);
				d.setDbo(last);
				//d.setIdHash(last.get("custId")==null? 0:last.get("custId").hashCode());
				d.setPostion((BSONTimestamp) last.get("dvs_server_ts"));
				list.add(d);
			}
			c.close();
			if(last != null){
				this.start = (BSONTimestamp) last.get("dvs_server_ts");
			}
			tt = System.currentTimeMillis() - tt;
			System.out.println("r:"+tt+" " + list.size());
			return list;
		};
		
		
		public Object call() throws Exception {
			dbc = mongo.getDB("dvs_test").getCollection("dvs_cust");
//			dbc = mongo.getDB("dvs_test").getCollection("dvs_test");
//			DBCollection dbc2 = mongo2.getDB("dvs_test").getCollection("dvs_cust");
			try {
				while (run) {
					List<Action> l = getList(5000);
					for (Action data : l) {
						getQueue(data.getHash()).put(data);
						
					}
					int size = l.size();
					if(size == 0){
						TimeUnit.MILLISECONDS.sleep(2000);
					}
					
				}
			} catch (InterruptedException e) {
//				e.printStackTrace();
				Thread.interrupted();
			}
			return null;
		}
	}
	
	class Work implements Callable{
		
		private DBCollection dbc = null;
		private BlockingQueue<Action> queue;
		private long count;
		
		public Work(BlockingQueue<Action> queue){
			this.queue = queue;
		}
		@Override
		public Object call() throws Exception {
			long l = 0;
			dbc = mongo.getDB("dvs_test").getCollection("dvs_cust2");
			
			try {
				while (run) {
					if("pool-1-thread-8".equals(Thread.currentThread().getName())){
						if(count >= 1000){
							System.err.println("pool-1-thread-8 over");
							throw new RuntimeException();
						}
					}
					
					Action d = queue.take();
					if(d != null){
						try {
							dbc.insert(d.getDbo());
						} catch (DuplicateKeyException e) {
							//System.err.print(e);
						}
						//positions.put(d.getCount(), d.getPostion());
						count++;
						if(count % 1000 == 0 ){
							System.out.println("w:"+Thread.currentThread().getName()+"_"+ (System.currentTimeMillis() - l));
						}else if(count % 1000 == 1 ){
							l = System.currentTimeMillis();
						}
					}
				}
			}catch (Throwable e){
//				e.printStackTrace();
				throw new RuntimeException(e);
			}
//			run = false;
			return null;
		}
	}
	
	 class Handler extends Observable implements SignalHandler {  
		  
	        @Override  
	        public void handle(Signal signal) {  
	            setChanged();  
	            notifyObservers(signal);  
	        }  
	  
	        /** 
	         *  
	         * @param signalName 
	         * @throws IllegalArgumentException 
	         */  
	        public void handleSignal(String signalName) throws IllegalArgumentException {  
	  
	            try {  
	  
	                Signal.handle(new Signal(signalName), this);  
	  
	            } catch (IllegalArgumentException x) {  
	  
	                throw x;  
	  
	            } catch (Throwable x) {  
	  
	                throw new IllegalArgumentException("Signal unsupported: "+signalName, x);  
	            }  
	        }  
	    }

	@Override
	public void update(Observable o, Object arg) {
		System.out.println("Received signal: " + arg); 
		if(arg instanceof Signal){
			Signal signal = (Signal)arg;
			System.out.println(signal.getName());
			System.out.println(signal.getNumber());
			if(arg != null && "SIGINT".equals(arg.toString())){
				run=false;
				pool.shutdown();
				System.exit(0);
			}
		}
	}  
	
}
