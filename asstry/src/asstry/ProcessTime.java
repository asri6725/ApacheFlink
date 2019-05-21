package asstry;

import java.sql.Date;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class ProcessTime {
	public static void main(String[] args) throws ParseException{
		String t1 = "23:05:00";
		String t2 = "22:06:00";
		
		SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

		java.util.Date date1 = format.parse(t1);
		java.util.Date date2 = format.parse(t2);
		long difference = date2.getTime() - date1.getTime(); 
		
		System.out.println(difference/1000/60);
	}
}
