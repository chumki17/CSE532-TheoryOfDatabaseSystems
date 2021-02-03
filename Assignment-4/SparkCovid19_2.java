import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;
import java.lang.Iterable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;


/* Spark imports */
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

public class SparkCovid19_2 {

	public static void main(String[] args) throws Exception {
		

		String input = args[0];
		String output = args[3];
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date start_date = dateFormat.parse(args[1]);
		Date end_date = dateFormat.parse(args[2]);
		Date date1 = dateFormat.parse("2019-12-01");
		Date date2 = dateFormat.parse("2020-04-30");
		if (start_date.before(date1) || start_date.after(date2) || end_date.after(date2) || end_date.before(date1)) {
			System.out.println("\n\n****************************************************************");
			System.out.println("Please Enter a valid date between range 2019-12-01 - 2020-04-30");
			System.out.println("****************************************************************\n");
			return;

		}

		SparkConf conf = new SparkConf().setAppName("Covid19_2");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Record> dataRDD = sc.textFile(args[0]).map(new Function<String, Record>() {
			public Record call(String line) throws Exception {
				String[] fields = line.split(",");
				Record sd = null;
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Date date = dateFormat.parse(fields[0]);
				int new_cases = Integer.parseInt(fields[2]);
				int new_deaths = Integer.parseInt(fields[3]);
				if (!(fields[1].trim().equalsIgnoreCase("location")) && !(fields[3].equals("new_deaths"))) {	
				 sd = new Record(date, fields[1], new_cases, new_deaths);
				}
				return sd;
			}
		});

		JavaPairRDD<String, Integer> counts = dataRDD.flatMapToPair(new PairFlatMapFunction<Record, String, Integer>() {

			public Iterator<Tuple2<String, Integer>> call(Record record) throws Exception {

				List<Tuple2<String, Integer>> countryCase = new ArrayList<Tuple2<String, Integer>>();

				Date current_date = record.getDate();
				int a = current_date.compareTo(start_date);
				int b = current_date.compareTo(end_date);
				if (a >= 0 && b <= 0) {

					countryCase.add(new Tuple2<String, Integer>(record.getLocation(), record.getNew_cases()));

				}
				return countryCase.iterator();

			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});

		counts.saveAsTextFile(output);

	}
}

class Record implements Serializable {
	static Date date;
	static String location;
	static int new_cases;
	static int new_deaths;

	public Record(Date date, String location, int new_cases, int new_deaths) {
		super();
		this.date = date;
		this.location = location;
		this.new_cases = new_cases;
		this.new_deaths = new_deaths;
	}

	public static Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public static String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public static int getNew_cases() {
		return new_cases;
	}

	public void setNew_cases(int new_cases) {
		this.new_cases = new_cases;
	}

	public static int getNew_deaths() {
		return new_deaths;
	}

	public void setNew_deaths(int new_deaths) {
		this.new_deaths = new_deaths;
	}

}
