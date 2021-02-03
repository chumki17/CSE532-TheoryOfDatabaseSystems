import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_2 {

	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		Path input = new Path(args[0]);
		Path output = new Path(args[3]);
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

		} else {
			c.set("start_date", args[1]);
			c.set("end_date", args[2]);
		}

		Job j = Job.getInstance(c, "Covid19_2");
		j.setJarByClass(Covid19_2.class);
		j.setMapperClass(MapForCovid19.class);
		j.setReducerClass(ReduceForCovid19.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class MapForCovid19 extends Mapper<LongWritable, Text, Text, IntWritable> {
		Text outputKey;
		IntWritable outputValue;

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			Configuration conf = con.getConfiguration();
			String line = value.toString();
			String[] CovidData = line.split(",");
			String date1 = conf.get("start_date");
			String date2 = conf.get("end_date");
			String dateRange = date1 + " - " + date2;
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date start_date, end_date, current_date;

			try {
				start_date = dateFormat.parse(date1);
				end_date = dateFormat.parse(date2);
				current_date = dateFormat.parse(CovidData[0].trim());
				int a = current_date.compareTo(start_date);
				int b = current_date.compareTo(end_date);

				if (a >= 0 && b <= 0) {
					if (!(CovidData[1].trim().equalsIgnoreCase("location")) && !(CovidData[3].equals("new_deaths"))) {
						this.outputKey = new Text(dateRange + "  " + CovidData[1].trim());
						this.outputValue = new IntWritable(Integer.parseInt(CovidData[3]));
						con.write(outputKey, outputValue);
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	public static class ReduceForCovid19 extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text country, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			con.write(country, new IntWritable(sum));
		}
	}
}
