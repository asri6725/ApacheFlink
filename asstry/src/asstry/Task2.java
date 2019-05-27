package asstry;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFields;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class Task2 {
	@SuppressWarnings("serial")
	@ReadFields("f1; f2")
	@ForwardedFields("f0->f0")
	public static class longconverter implements MapFunction<Tuple3<String, String, String>, Tuple2<String, Long>> {
		  @Override
		  public Tuple2<String, Long> map(Tuple3<String, String, String> in) throws ParseException {
			  //convert calculate scheduled departure - actual departure in minutes and store it as <carriercode, delay>	
			  
			  String t1 = in.f1;
			  String t2 = in.f2;
				
				SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

				java.util.Date date1 = format.parse(t1);
				java.util.Date date2 = format.parse(t2);
				long difference = date2.getTime() - date1.getTime(); 
				difference = ((difference/1000)/60);
		    return new Tuple2<String, Long>(in.f0, difference);
		  }
		}
	// map function to join tuples
	@SuppressWarnings("serial")
	@ForwardedFields("f0.f1->f0; f1.f1->f1")
	public static class cleantuple implements MapFunction<Tuple2<Tuple3<String, String, String>, Tuple2<String, Long>>, Tuple2<String, Long>> {
		  @Override
		  public Tuple2<String, Long> map(Tuple2<Tuple3<String, String, String>, Tuple2<String, Long>> input) {
		    return new Tuple2<String, Long>(input.f0.f1, input.f1.f1);
		  }
		}

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		  	//get output file
			String output_filepath = "C:/Users/Abhi/Desktop/test.csv";
			
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
			//filter country to united states
			DataSet<Tuple3<String, String, String>> planes = airlines.filter(new FilterFunction<Tuple3<String, String, String>>() {
                public boolean filter(Tuple3<String, String, String> entry) {return entry.f2.equals("United States"); }
         });
			
			//read aircrafts
			
			DataSet<Tuple3<String, String, String>> aircraft = 
					env.readCsvFile(inpath+inpa2)
					.includeFields("010000010100")
					.ignoreFirstLine()
					.ignoreInvalidLines()
					.types(String.class, String.class, String.class);
			// remove empty(cancelled) flights by checking sizee
		DataSet<Tuple3<String, String, String>> aaircraft = aircraft.filter(new FilterFunction<Tuple3<String, String, String>>() {
                public boolean filter(Tuple3<String, String, String> entry) {return entry.f1.length()>3 && entry.f2.length()>3; }
         });

			// check the actual from scheduled and find delay in minutes
			DataSet<Tuple2<String, Long>> aircraftproper = aaircraft.map(new longconverter());
			
			// remove -ve or 0 "delays" as they arent delays
			DataSet<Tuple2<String, Long>> flights = aircraftproper.filter(new FilterFunction<Tuple2<String, Long>>() {
                public boolean filter(Tuple2<String, Long> entry) {return entry.f1>0; }
         });
			//join with airlines
			DataSet<Tuple2<Tuple3<String, String, String>, Tuple2<String, Long>>> result = 
					planes.join(flights)
					.where(0)
					.equalTo(0);
			
			// I have a joint table with airlines joined to flights with delay in minutes
			// Here I try to remove the tuple2<Tuple3<>Tuple2<>> format
			// And make it to To one tuple of <name, delays>
			DataSet<Tuple2<String, Long>> promislast = result.map(new cleantuple());
			
			// get a TableEnvironment
		    BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		    
		    Table out = tableEnv.fromDataSet(promislast, "name, delay");
			
		    //do the delay count, average, min, max 
		    Table res = out.groupBy("name").select("name, delay.count as count, delay.avg as average, delay.min as min, delay.max as max");
		    res = res.orderBy("name");
		    // output final result
		    res.writeToSink(new CsvTableSink(output_filepath, "\t", 1, WriteMode.OVERWRITE));

		    
			//promislast.writeAsCsv(output_filepath, WriteMode.OVERWRITE);
			env.execute("Executing task2 opt");
			//Thread.sleep(20000);
			
	  }
}