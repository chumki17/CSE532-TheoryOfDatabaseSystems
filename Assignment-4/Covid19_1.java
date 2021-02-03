import java.io.IOException;
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

public class Covid19_1 {
	
	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		Path input = new Path(args[0]);
		Path output = new Path(args[2]);
		c.set("World", args[1]);
		Job j = Job.getInstance(c, "Covid19_1");
		j.setJarByClass(Covid19_1.class);
		j.setMapperClass(MapForCovid19.class);
		j.setReducerClass(ReduceForCovid19.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class MapForCovid19 extends Mapper<LongWritable, Text, Text, IntWritable> {
		Text outputKey1 = null;
		Text outputKey2 = null;
		IntWritable outputValue;

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			Configuration conf = con.getConfiguration();
			String param = conf.get("World");
			String line = value.toString();
			String[] CovidData = line.split(",");

			if (!(CovidData[1].trim().equalsIgnoreCase("location")) && !(CovidData[2].equals("new_cases"))) {
				this.outputKey1 = new Text(CovidData[1].trim());

				if (!(CovidData[1].trim().equalsIgnoreCase("World"))) {
					this.outputKey2 = new Text(CovidData[1].trim());
				}
				outputValue = new IntWritable(Integer.parseInt(CovidData[2]));
				if (param.equals("true")) {
					con.write(outputKey1, outputValue);
				} else {
					con.write(outputKey2, outputValue);

				}

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