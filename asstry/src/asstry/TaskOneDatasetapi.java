package asstry;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.utils.ParameterTool;


import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.*;


public class TaskOneDatasetapi {
	public static void main(String[] args) throws Exception{
		//get output file
		String output_filepath = "C:/Users/Abhi/Desktop/test.csv";
		
		//input file path
		String infile = "C:/Users/Abhi/Desktop/";
		//obtain handle to execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//define a sid dataset
		DataSet<Tuple2<Integer, String>> names = 
				env.readCsvFile(infile+"book2.csv")
				.includeFields("11")
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.types(Integer.class, String.class);
		/*
		
		DataSet<Tuple2<Integer, Integer>> sids = 
				env.readCsvFile(infile+"book1.csv")
				.includeFields("11")
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.types(Integer.class, Integer.class);
		
		DataSet<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, String>>> joined = sids.join(names)
				.where(0).equalTo(0);
		
		*//*
		joined.writeAsCsv(output_filepath, WriteMode.OVERWRITE);
		*/
		
		// get a TableEnvironment
	    BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
	    
	    Table out = tableEnv.fromDataSet(names, "id, name");
	    
//		Table out = in.groupBy("id").select("id, name");
	    
	    // output final result
	    out.writeToSink(new CsvTableSink(output_filepath, ",", 1, WriteMode.OVERWRITE));

		
		env.execute("Testing !!!");
		Thread.sleep(20000);
	}
}
