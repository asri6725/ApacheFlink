package asstry;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.sources.*;
import org.apache.flink.table.sinks.*;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.utils.ParameterTool;



public class Taskone {
	
	
  public static void main(String[] args) throws Exception {
    // get output file command line parameter - or use "descan.txt" as default
    final ParameterTool params = ParameterTool.fromArgs(args);
    String output_filepath = params.get("output", "C:/Users/Abhi/Desktop/descans.txt");
    String inpath = "C:/Users/Abhi/Desktop/assignment_data_files/";

    // obtain handle to execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    
    // get a TableEnvironment
    BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
    
    TableSource aircraft = CsvTableSource.builder().path(inpath+"ontimeperformance_aircrafts.csv").
    					ignoreFirstLine()
    					.ignoreParseErrors()
    					.field("tailnumber", Types.STRING)
    					.field("useless", Types.STRING)
    					.field("manufacturer",  Types.STRING)
    					.field("useless_a1", Types.STRING)
    					.field("model", Types.STRING)
    					.build();
    tableEnv.registerTableSource("tailnumber", aircraft);
    TableSource flights = CsvTableSource.builder().path(inpath+"ontimeperformance_flights_tiny.csv")
    		.ignoreFirstLine()
			.ignoreParseErrors()
			.field("flight_id", Types.INT)
			.field("useless1", Types.STRING)
			.field("useless2", Types.STRING)
			.field("useless3", Types.STRING)
			.field("useless4", Types.STRING)
			.field("useless5", Types.STRING)
			.field("tail_numberflights", Types.STRING)
			.build();
tableEnv.registerTableSource("flightid", flights);
    
    Table airc = tableEnv.scan("tailnumber").where("manufacturer = 'CESSNA'");
   
    Table flig = tableEnv.scan("flightid");
    
    
    
    Table orderedresult = airc.join(flig).where("tailnumber = tail_numberflights").select("manufacturer, model");
    
    Table result = orderedresult.groupBy("model").select("model, model.count as frequency");
    result.orderBy("model, frequency");
    //Table result = orderedresult.select("COUNT(model)");
    
    // output final result
    result.writeToSink(new CsvTableSink(output_filepath, "\t", 1, WriteMode.OVERWRITE));

    // output execution plan
    //System.out.println(env.getExecutionPlan());

    // execute the FLink job
    env.execute("Executing sample1(TableAPI) program");

    
    // alternatively: get execution plan
    // System.out.println(env.getExecutionPlan());

    // wait 20secs at end to give us time to inspect ApplicationMAster's WebGUI
    Thread.sleep(20000);
  }
}
