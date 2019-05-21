package asstry;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.fs.s3hadoop.shaded.org.apache.commons.beanutils.converters.LongConverter;
import org.apache.flink.api.java.utils.ParameterTool;

public class Userratinganalysis {

	public static class longconverter implements MapFunction<Tuple2<String, String>, Tuple1<Long>> {
		  @Override
		  public Tuple1<Long> map(Tuple2<String, String> in) throws ParseException {

			  String t1 = in.f0;
			  String t2 = in.f1;
				
				SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

				java.util.Date date1 = format.parse(t1);
				
				java.util.Date date2 =  format.parse(t2);
				long difference = date2.getTime() - date1.getTime(); 

				difference = ((difference/1000)/60);
		    return new Tuple1<Long>(difference);
		  }
		}
	public static void main(String[] args) throws Exception {
		//get output file
		String output_filepath = "C:/Users/Abhi/Desktop/test.csv";
		
		//input file path
		String inpath = "C:/Users/Abhi/Desktop/";
	    String inpa1 = "ontimeperformance_flights_tiny.csv";
	    String inpa2 = "testing.csv";
	    
		//obtain handle to execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		
		DataSet<Tuple2<String, String>> aircraft = 
				env.readCsvFile(inpath+inpa2)
				.ignoreFirstLine()
				.includeFields("11")
				.ignoreInvalidLines()
				.types(String.class, String.class);
		
		DataSet<Tuple2<String, String>> aaircraft = aircraft.filter(new FilterFunction<Tuple2<String, String>>() {
            public boolean filter(Tuple2<String, String> entry) {return entry.f0.length()>3 && entry.f1.length()>3; }
     });
		DataSet<Tuple1<Long>> convertpls = aaircraft.map(new longconverter());
		
		
		convertpls.writeAsCsv(output_filepath,WriteMode.OVERWRITE);
		
		
		env.execute("Executing program");
	}

}


