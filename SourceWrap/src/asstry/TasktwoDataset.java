package asstry;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.net.nntp.Threadable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class TasktwoDataset {
	@SuppressWarnings("serial")
	public static class longconverter implements MapFunction<Tuple3<String, String, String>, Tuple4<String, String, String, Long>> {
		  public Tuple4<String, String, String, Long> map(Tuple3<String, String, String> in) throws ParseException {
			  //convert calculate scheduled departure - actual departure in minutes and store it as <carriercode, delay>	
			  
			  String t1 = in.f1;
			  String t2 = in.f2;
			  long difference = 0l;
			  if(t1.length()==0 || t2.length()==0)
				  difference = -1l;
			  else{
				SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

				java.util.Date date1 = format.parse(t1);
				java.util.Date date2 = format.parse(t2);
				difference = date2.getTime() - date1.getTime(); 
				difference = ((difference/1000)/60);
			  }
		    return new Tuple4<String, String, String, Long>(in.f0, t1, t2, difference);
		  }
		}
	// map function to join tuples
	@SuppressWarnings("serial")
	public static class cleantuple implements MapFunction<Tuple2<Tuple3<String, String, String>, Tuple4<String, String, String, Long>>, Tuple3<String, String, Long>> {
		  @Override
		  public Tuple3<String, String, Long> map(Tuple2<Tuple3<String, String, String>, Tuple4<String, String, String, Long>> input) {
		    
			  return new Tuple3<String, String,  Long>(input.f0.f1, input.f0.f2 ,input.f1.f3);
		  }
		}

	public static void main(String[] args) throws Exception {
		  	//get output file
			String output_filepath = "C:/Users/Abhi/Desktop/testslow.csv";
			
			//input file path
			String inpath = "C:/Users/Abhi/Desktop/assignment_data_files/";
		    String inpa1 = "ontimeperformance_airlines.csv";
		    String inpa2 = "ontimeperformance_flights_medium.csv";
		    
			//obtain handle to execution environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			//read the airlines file
			DataSet<Tuple3<String, String, String>> airlines = 
					env.readCsvFile(inpath+inpa1)
					.includeFields("111")
					.ignoreFirstLine()
					.ignoreInvalidLines()
					.types(String.class, String.class, String.class);
			
			//read aircrafts
			
			DataSet<Tuple3<String, String, String>> aircraft = 
					env.readCsvFile(inpath+inpa2)
					.includeFields("010000010100")
					.ignoreFirstLine()
					.ignoreInvalidLines()
					.types(String.class, String.class, String.class);
			
			// check the actual from scheduled and find delay in minutes
			DataSet<Tuple4<String, String, String, Long>> aircraftproper = aircraft.map(new longconverter());
			
			
			DataSet<Tuple2<Tuple3<String, String, String>, Tuple4<String, String, String, Long>>> result = 
					airlines.join(aircraftproper)
					.where(0)
					.equalTo(0);
			
			// I have a joint table with airlines joined to flights with delay in minutes
			// Here I try to remove the tuple2<Tuple3<>Tuple2<>> format
			// And make it to To one tuple of <name, country, delays>
			DataSet<Tuple3<String, String, Long>> promislast = result.map(new cleantuple());
			
			// get a TableEnvironment
		    BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		    
		    Table out = tableEnv.fromDataSet(promislast, "name, countryname, delay");
			
		    //do the delay count, average, min, max 
		    Table out1 = out.where("countryname = 'United States'").where("delay>0").select("name, delay");
			
		    
		    Table res = out1.groupBy("name").select("name, delay.count as count, delay.avg as average, delay.min as min, delay.max as max");
		    res = res.orderBy("name");
		    // output final result
		    res.writeToSink(new CsvTableSink(output_filepath, "\t", 1, WriteMode.OVERWRITE));

		    
			//promislast.writeAsCsv(output_filepath, WriteMode.OVERWRITE);
			env.execute("Executing task2 unopt");
			//Thread.sleep(20000);
			
	  }
}