package asstry;

import java.awt.Composite;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.sources.*;
import org.apache.flink.table.sinks.*;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.utils.ParameterTool;


public class Tasktwo {
	 public static void main(String[] args) throws Exception {
		    // get output file command line parameter - or use "top_rated_users.txt" as default
		    final ParameterTool params = ParameterTool.fromArgs(args);
		    String output_filepath = params.get("output", "C:/Users/Abhi/Desktop/test.txt");
		    String inpath = "C:/Users/Abhi/Desktop/assignment_data_files/";
		    String inpa1 = "ontimeperformance_airlines.csv";
		    String inpa2 = "ontimeperformance_flights_tiny.csv";
		    // obtain handle to Flink's execution environmnet
		    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		    // get a TableEnvironment
		    BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		    
		    TableSource airlines = CsvTableSource.builder()
		    		.path(inpath+inpa1)
		    		.ignoreFirstLine()
		    		.ignoreParseErrors()
		    		.field("carrier_code", Types.STRING())
		    		.field("carrier_name", Types.STRING())
		    		.field("carrier_country", Types.STRING())
		    		.build();
		    tableEnv.registerTableSource("carrier_code", airlines);
		    
		    TableSource flights = CsvTableSource.builder()
		    		.path(inpath+inpa2)
		    		.ignoreFirstLine()
		    		.ignoreParseErrors()
		    		.field("flightid", Types.STRING())
		    		.field("carriercode", Types.STRING())
		    		.field("useless1", Types.STRING())
		    		.field("useless2", Types.STRING())
		    		.field("useless3", Types.STRING())
		    		.field("useless4", Types.STRING())
		    		.field("useless5", Types.STRING())
		    		.field("scheduled_departure", Types.SQL_TIME())
		    		.field("scheduled_arrival", Types.SQL_TIME())
		    		.field("actual_departure", Types.SQL_TIME())
		    		.field("actual_arrival", Types.SQL_TIME())
		    		.field("", Types.INT())
		    		.build();
		    tableEnv.registerTableSource("flightid", flights);
		    
		    
		    Table liners = tableEnv.scan("carrier_code").where("carrier_country = 'United States'").select("carrier_code as code, carrier_name");   // works
		    
		    Table planes = tableEnv.scan("flightid").where("scheduled_departure < actual_departure").select("scheduled_departure, actual_departure, carriercode");
		    
		    Table bigtable = liners.join(planes).where("carriercode = code").select("carrier_name, scheduled_departure, actual_departure");
		    
		    //Table time = bigtable.select("actual_departure, scheduled_departure").where("scheduled_departure - actual_departure > 0.minutes");
		    
		    Table numberdelays = bigtable.groupBy("carrier_name").select("carrier_name, carrier_name.count as frequency");		    
		    numberdelays.writeToSink(new CsvTableSink(output_filepath, "\t", 1, WriteMode.OVERWRITE));
		    env.execute("Executing program");
		    Thread.sleep(20000);

	 }
}
