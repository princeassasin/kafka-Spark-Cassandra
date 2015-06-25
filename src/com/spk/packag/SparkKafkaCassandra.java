package com.spk.packag;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

import scala.Tuple2;

//import org.apache.hadoop.hdfs.server.namenode.Content.Counts;
//import org.apache.log4j.Logger;
//import org.apache.log4j.Level;
import com.google.common.collect.Lists;
//import com.google.common.base.Equivalence;
//import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.examples.streaming.StreamingExamples
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.*;
//import static com.datastax.spark.connector.CassandraJavaUtil.*;
import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.Connection.Factory;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.common.base.Optional;
//import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
//import scala.Tuple2;
import java.io.File;
import java.io.Serializable;
import java.util.*;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.crypto.dsig.XMLObject;
import javax.xml.crypto.XMLStructure;

public class SparkKafkaCassandra {

	 private static final Pattern SPACE = Pattern.compile(" ");

	 // protected static final Pattern SPACE = Pattern.compile(" ");

	private SparkKafkaCassandra() { }
	//private Cluster cluster;
	//private Session session;
	//public interface XMLObject extends XMLStructure{
	  public static void main(String[] args) {
		  
	    if (args.length < 4) {
	      System.err.println("Usage: Need to Pass KafKa Ip, Spark Ip, Cassandra Ip, Topic");
	      System.exit(1);
	      }
	    
String kafkaIp=args[0];
String sparkIp=args[1];
final String cassandraIp=args[2];
String myTopic=args[3];
String consumerGroup="my-consumer-group";

int numThreads =1;

	   // StreamingExamples.setStreamingLogLevels();
	    //SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
	    //sparkConf.setMaster("spark://60f81dc6426c:7077");
	   // SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount").setMaster("spark://60f81dc6426c:7077");

	    // Create the context with a 1 second batch size
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkIp,"SparkKafkaCassandra", new Duration(2000), System.getenv("SPARK_HOME"), JavaStreamingContext.jarOfClass(SparkKafkaCassandra.class));

	   // int numThreads = Integer.parseInt(args[3]);
	    //Logger.getLogger("org").setLevel(Level.OFF);
	    //Logger.getLogger("akka").setLevel(Level.OFF);
	    Map<String, Integer> topicMap = new HashMap<String, Integer>();
	    String[] topics = myTopic.split(",");
	    for (String topic: topics) {
	        topicMap.put(topic, numThreads);
	      }
	System.out.println("Topics" + topicMap);    
	    
	    JavaPairReceiverInputDStream<String, String> messages =
	            KafkaUtils.createStream(jssc, kafkaIp, consumerGroup, topicMap);
	    System.out.println("Connection !!!!" + kafkaIp);
	   /* JavaDStream<String> data = messages.map(new Function<Tuple2<String, String>, String>() 
                {
                    /**
					 * 
					 */
					/*private static final long serialVersionUID = 1L;

					public String call(Tuple2<String, String> message)
                    {
                        return message._2();
                    }
                }
                );*/
	    
	    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
	        @Override
	        public String call(Tuple2<String, String> tuple2)
	        {
	        	return tuple2._2();
	          
	        }
	      });

	     JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	        @Override
	        public Iterable<String> call(String x) {
	          return Lists.newArrayList(SPACE.split(x));
	        }
	      });

	      JavaPairDStream<String, Integer> css = words.mapToPair(
	        new PairFunction<String, String, Integer>() {
	          @Override
	          public Tuple2<String, Integer> call(String s) {
	        	  /*try{
	        		  JAXBContext context = JAXBContext.newInstance(s);
	                  Unmarshaller un = context.createUnmarshaller();
	                  String id = (String) un.unmarshal(new File(s));
	                 // XMLObject xml=new XMLObject(s);
		        		//String id = XMLObject.getId();
		        		//return new Tuple2<String,Integer>(id,context.toString());
	        		  
	        		  
	                    
		        	}catch(Exception ex){
		        		ex.printStackTrace();
		        		return new Tuple2<String, Integer>(s,1);
		        	}*/
	            return new Tuple2<String, Integer>(s, 1);
	          }
	          }).reduceByKey(
      				new Function2<Integer, Integer, Integer>(){
      		@Override		   
		public Integer call(Integer i1, Integer i2){
  		 try{
  			 int sum =i1+i2;
  			 return sum;
  		 }catch(Exception ex){
  			 ex.printStackTrace();
  			 return 0;
  		 }
  	 }  	
  });
	     /* JavaPairDStream<String, Integer> counts= css.reduce(new Function2<Integer, Integer, Integer>() {
	        @Override
	          public Integer call(Integer i1, Integer i2) {
	           try{int sum =i1+i2;
  			 return sum;
	           }catch(Exception ex) {
	        	   ex.printStackTrace();
	        	   return 0;
	           }
	          }
	        });
	     /* JavaPairDStream<String, Integer> counts= css.reduceByKeyAndWindow(
	    		  new Function2<String, String, String>() {
		public String call(String i1, String i2) {
	           try{
	        	   return i1;
	           }catch(Exception ex) {
	        	   ex.printStackTrace();
	        	   return i1;
	           }
	          }
	  }.new Duration(5 * 60 * 1000).new Duration(3*1000).map(
	        		new PairFunction<Tuple2<String, String>, String, Integer>(){

						@Override
						public Tuple2<String, Integer> call(
								Tuple2<String, String> t) throws Exception {
							// TODO Auto-generated method stub
							String val=t._2();
							JAXBContext context = JAXBContext.newInstance(val);
			                  Unmarshaller un = context.createUnmarshaller();
			                  String market = (String) un.unmarshal(new File(val));
							//XMLObject xml= new XMLObject(val);
							//String market= XMLObject.getEncoding();
							return new Tuple2<String, Integer>(market, 1); 
						}
	        		}).reduceByKey(
	        				new Function2<Integer, Integer, Integer>(){
	   
			public Integer call(Integer i1, Integer i2){
	    		 try{
	    			 int sum =i1+i2;
	    			 return sum;
	    		 }catch(Exception ex){
	    			 ex.printStackTrace();
	    			 return 0;
	    		 }
	    	 }  	
	    }));*/
	        		
	    
	     css.foreachRDD (new Function <JavaPairRDD<String, Integer>, Void>(){ 
	    	
	    	 /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
	    	public Void call(JavaPairRDD<String, Integer> rdd) throws Exception{
	    		

		    	//String cassandraIp = null;
				Cluster cluster = Cluster.builder()
		                .addContactPoint( cassandraIp).build();
		    	Session session = cluster.connect();
		    	Metadata metadata = cluster.getMetadata();
		    	System.out.printf("Connected to cluster: HELLO!!\n", 
		                metadata.getClusterName());
		    	
		    	for(Tuple2<String, Integer> t: rdd.collect()){
		    		//Tuple2<String, Integer> t;
					System.out.println("---for each"+ t._1() +","+ t._2());
		    		
		    		String word= t._1();
		    		Integer count= t._2();
		    	try{
		    		
		    	String sql="insert into wordcount.samplewordcount ( word, count )"+ 
		    			"VALUES ( '" +word+"', '"+count+"')";
		    	/*String sql="INSERT INTO simplex9.playlists (id, song_id,title, album,artist ) " +
			      "VALUES (" +
			          "7ad54392-bcdd-35a6-8417-4e047860b377," +
			          "296e9c04-9bec-3085-827d-c17d3df2122a," +
			          "'jjrfjbk'," +
			          "'jggknk'," +
			          "'gjkn'" +
			          ");";*/
				          System.out.println(sql);
		    	session.execute(sql);
		    	}catch(Exception ex)
		    	{
		    	System.out.println("Error: " + ex.getMessage());	
		    	ex.printStackTrace();
		    	}
	    	}

		    	
	    	
	    	
cluster.close();
return null;
	    
	    	}
	     }
	     );	
	    		


	    css.print();
	    jssc.start();
	    jssc.awaitTermination();
	  
}
}