package asstry;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.api.common.operators.Order;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;


public class Task1 {

  @SuppressWarnings("serial")
public static <R> void main(String[] args) throws Exception {

 
    String fileloc = "/C:/Users/Abhi/Desktop/assignment_data_files/"; //data file locations
    String output_filepath = "C:/Users/Abhi/Desktop/Task1res.csv"; //result output location
    
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(); 
   
    DataSet<Tuple4<String, String, String, Integer>> aircrafts =
    	      env.readCsvFile(fileloc+"ontimeperformance_aircrafts.csv")
    	      .includeFields("101010001")// Take tail number, manufacturer, model no, and year so we can edit this to 1 for summation later
    	      .ignoreFirstLine()
    	      .ignoreInvalidLines()
    	      .types(String.class, String.class, String.class, Integer.class);
    
   
	DataSet<Tuple4<String, String, String, Integer>> cessnaaircraft = 
    		aircrafts.filter(new FilterFunction<Tuple4<String,String, String, Integer>>(){
    	     public boolean filter(Tuple4<String,String, String, Integer> entry) {
    	    	   return entry.f1.equals("CESSNA"); //filter only planes manufactured by Cessna
    	    }
    });
	
	DataSet<Tuple4<String, String, String, Integer>> cessnacount = cessnaaircraft.flatMap(new cesMapper()); //Change CESSNA to Cessna and change year to 1 
    		
	
     DataSet<Tuple1<String>> flights =
       env.readCsvFile(fileloc+"ontimeperformance_flights_medium.csv")
       .includeFields("000000100000") // Take tail number only
       .ignoreFirstLine()
       .ignoreInvalidLines()
       .types(String.class);
 
     
    DataSet<Tuple3<String, String,Integer>> joinresult =
    		flights.join(cessnacount).where(0).equalTo(0) //join flights with tail number so we get all the flights
    		.projectSecond(1,2,3); //only copy manufacturer, model no and 1

    DataSet<Tuple2< String,Integer>> result = joinresult.flatMap(new concats()); // Concatenates Cessna with model no and reduces model no to 3 digits  
    
    result.groupBy(0)	 //group each model
    .sum(1)    			 // count how number of each model
    .sortPartition(1, Order.DESCENDING) // order models by most popular
    .first(3);   //only output the top 3 models 
    
    BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
    Table write = tableEnv.fromDataSet(result,"airline,models");
    write.writeToSink(new CsvTableSink(output_filepath,"\t",1,WriteMode.OVERWRITE));
    // execute the FLink job
    env.execute("Executing task 1 opt");
		
    		
   
    // wait 20secs at end to give us time to inspect ApplicationMaster's WebGUI
    //Thread.sleep(20000);
  }
  @SuppressWarnings("serial")
  private static class cesMapper implements FlatMapFunction<Tuple4<String, String, String, Integer>, Tuple4<String, String, String, Integer>> {
	   public void flatMap( Tuple4<String, String, String,Integer> input_tuple, Collector<Tuple4<String, String, String, Integer>> out) {
	    	out.collect(new Tuple4<String, String, String,Integer> (input_tuple.f0,"Cessna",input_tuple.f2, 1)); 
	  }
  }
  
  @SuppressWarnings("serial")
  private static class concats implements FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>{
	  public void flatMap( Tuple3<String, String, Integer> input_tuple, Collector<Tuple2<String, Integer>> output) {
		  String model = input_tuple.f0.concat(" ");
		  model = model.concat(input_tuple.f1); // adds model no to manufacturer string
		  model = StringUtils.left(model, 10); // shortens string to 10 digits
		  output.collect(new Tuple2<String,Integer> (model, input_tuple.f2));
	  }
  }
 
}
