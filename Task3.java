package asstry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;

public class Task3 {
	
	@SuppressWarnings("serial")
	public static <R> void main(String[] args) throws Exception {

		
	    String fileloc = "/C:/Users/Abhi/Desktop/assignment_data_files/"; //data file locations
	    String output_filepath = "/C:/Users/Abhi/Desktop/Task3.csv"; //result output location
	    // get country or use USA
	    final ParameterTool params = ParameterTool.fromArgs(args);
	    String country = params.get("country", "USA");

	    
	    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(); 
	   
	    DataSet<Tuple2<String, String>> airports =
	    	      env.readCsvFile(fileloc+"ontimeperformance_airports.csv")
	    	      .includeFields("10001")// Take airport code, country
	    	      .ignoreFirstLine()
	    	      .ignoreInvalidLines()
	    	      .types(String.class, String.class);
	    
	    DataSet<Tuple2<String, String>> cairports = 
	    		airports.filter(new FilterFunction<Tuple2<String,String>>(){
	    	     public boolean filter(Tuple2<String,String> entry) {
	    	    	   return entry.f1.equals(country);} //filter only airports in the country
	    });
		
		DataSet<Tuple3<String, String, Integer>> cairportss = cairports.flatMap(new airportmapper()); 
	    		
		
	     DataSet<Tuple3<String,String, String>> flights =
	       env.readCsvFile(fileloc+"ontimeperformance_flights_small.csv")
	       .includeFields("010010100000") // Take flight id, origin and tail number
	       .ignoreFirstLine()
	       .ignoreInvalidLines()
	       .types(String.class, String.class, String.class);

	     
	    DataSet<Tuple2<String,Integer>> joinresult =
	    		flights.join(cairportss).where(1).equalTo(0) //join by airport code
	    		.projectFirst(0,2);
	    
	    DataSet<Tuple3<String, String, String>> aircrafts =
	    	      env.readCsvFile(fileloc+"ontimeperformance_aircrafts.csv")
	    	      .includeFields("101010000")// Take tail number, manufacturer, model no, and year so we can edit this to 1 for summation later
	    	      .ignoreFirstLine()
	    	      .ignoreInvalidLines()
	    	      .types(String.class, String.class, String.class);
		
		DataSet<Tuple2< String,String>> aircraftcon = aircrafts.flatMap(new concats()); // Concatenates Cessna with model no and reduces model no to 3 digits
	    
	    DataSet<Tuple3<String, String,Integer>> aircraftjoin =
	    		aircraftcon.join(joinresult).where(1).equalTo(1) //join flights with tail number so we get all the flights
	    		.projectFirst(0) // plane make
	    		.projectSecond(0,1); //airline code, 1airport
	    
	   
	    DataSet<Tuple2<String, String>> airlines =
	    	      env.readCsvFile(fileloc+"ontimeperformance_airlines.csv")
	    	      .includeFields("110")// Takecode, name
	    	      .ignoreFirstLine()
	    	      .ignoreInvalidLines()
	    	      .types(String.class, String.class);  
	    
	    DataSet<Tuple2<String, String>> planeair =
	    		aircraftjoin.join(airlines).where(1).equalTo(0) //join flights with tail number so we get all the flights
	    		.projectSecond(1) //airline code, airport
	    		.projectFirst(0); // plane make
	    		
	    
	    DataSet<Tuple3< String,String, Integer>> add1a = planeair.flatMap(new add1());

	    
	    BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
	    
	    Table out = tableEnv.fromDataSet(add1a, "airline, planemodel, count");
		
	    //count the number and sort by airline, model
	    Table res = out.groupBy("airline, planemodel").select("airline, planemodel, planemodel.count as number");
	    
	    // Convert the table back to DS *sigh*
	    TupleTypeInfo<Tuple3<String, String, Long>> tupleType = new TupleTypeInfo<>(
	    		  Types.STRING(),
	    		  Types.STRING(),
	    		  Types.LONG());
	    DataSet<Tuple3<String, String, Long>> resultds = 
	    		  tableEnv.toDataSet(res, tupleType);
	    
	    
	    DataSet<Tuple3<String, String, Long>> resultds2 = resultds;
	    // iterate of resultds once using resultds 2
	    DataSet<Tuple2<String, List<String>>> resultds3 = resultds2.map(new RichMapFunction<Tuple3<String, String, Long>, Tuple2<String, List<String>>>() {
	    	  
	    	private Collection<Tuple3<String, String, Long>> broadcastSet; //contains resultds
	     
	        public void open() {
	        	this.broadcastSet = getRuntimeContext().getBroadcastVariable("resultds");
	        }
	             
			public Tuple2<String, List<String>> map(Tuple3<String, String, Long> arg0) throws Exception {
				open();
				
				List<checker> list = new ArrayList<checker>();
				// iterate again over the dataset
				for(Tuple3<String, String, Long> val: this.broadcastSet){ //for each tuple in resultds2, iterate over resultds
					if(val.f0.equals(arg0.f0))
						list.add(new checker(val.f1, val.f2));   							// add current tuple's airline's model to a list
				}
				// sort the list
				Collections.sort(list, new Comparator<checker>() {
					@Override
					public int compare(checker arg0, checker arg1) {
						Long val = arg1.f1 - arg0.f1;					// i think arg0-arg1 sorts ascending, arg1-arg0 sorts descending
						return val.intValue();
					}
				});
				
				List<String> list1 = new ArrayList<String>();
				//choose top 5 models
				Iterator<checker> iter = list.iterator();
				
				int count = 0;
				
				while(iter.hasNext()){
					list1.add(iter.next().f0);
					count++;
					if(count+1 == 5)
						break;
				}
				
				return new Tuple2<String, List<String>>(arg0.f0, list1); //return the list of tuples back
			}
	      
	    }).withBroadcastSet(resultds, "resultds"); // resultds is broadcasted
	    
	    DataSet<Tuple2<String, List<String>>> resultds4 = resultds3.distinct(0); // for distinct fields, doesn't work
	      
	    resultds4
	    .writeAsCsv(output_filepath, WriteMode.OVERWRITE).setParallelism(1);
	    
	    // execute the FLink job
	    env.execute("Executing task 3");
	  }

	@SuppressWarnings("serial")
	  private static class airportmapper implements FlatMapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {
		   public void flatMap( Tuple2<String, String> input_tuple, Collector<Tuple3<String, String, Integer>> out) {
		    	out.collect(new Tuple3<String, String, Integer> (input_tuple.f0,input_tuple.f1, 1)); 
		  }
	  }
	  
	  @SuppressWarnings("serial")
	  private static class add1 implements FlatMapFunction<Tuple2<String, String>, Tuple3<String,String, Integer>>{
		  public void flatMap( Tuple2<String, String> input_tuple, Collector<Tuple3<String, String, Integer>> output) {
			  output.collect(new Tuple3<String,String,Integer> (input_tuple.f0, input_tuple.f1,1));
		  }
	  }
	  
	  @SuppressWarnings("serial")
	  private static class concats implements FlatMapFunction<Tuple3<String, String, String>, Tuple2<String, String>>{
		  public void flatMap( Tuple3<String, String, String> input_tuple, Collector<Tuple2<String, String>> output) {
			  String model = input_tuple.f1.concat(" ");
			  model = model.concat(input_tuple.f2); // adds model no to manufacturer string
			  
			  output.collect(new Tuple2<String,String> (model, input_tuple.f0));
		  }
	  }
	  public static class stringextractor implements MapFunction<Tuple3<String, String, Long>, String> {
		 
		@Override
		public String map(Tuple3<String, String, Long> arg0) throws Exception {
			return arg0.f1;
			}
		}
	  public static class checker{
		  public String f0;
		  public long f1;
		  public checker(String val0, long val1){ this.f0 = val0; this.f1 = val1;}
	  } 
}
