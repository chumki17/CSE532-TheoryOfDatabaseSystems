import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_3 {
	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		Path input = new Path(args[0]);
		Path popFilePath = new Path(args[1]);
		Path output = new Path(args[2]);
		Job j = Job.getInstance(c, "Covid19_3");
		j.addCacheFile(new URI(popFilePath.toString()));
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
		Text outputKey = null;
		IntWritable outputValue;

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] CovidData = line.split(",");
			if (!(CovidData[1].trim().equalsIgnoreCase("location")) && !(CovidData[2].equals("new_cases"))) {
				this.outputKey = new Text(CovidData[1].trim());
				this.outputValue = new IntWritable(Integer.parseInt(CovidData[2]));
				con.write(outputKey, outputValue);
			}
		}
	}

	public static class ReduceForCovid19 extends Reducer<Text, IntWritable, Text, Text> {
		HashMap<String, String> popData = null;
		public void setup(Context context) throws IOException, InterruptedException {
			URI[] cacheFiles = context.getCacheFiles();
			Configuration conf = context.getConfiguration();
			this.popData = new HashMap<String, String>();
			if (cacheFiles != null && cacheFiles.length > 0) {
				for (URI file : cacheFiles) {

					try {
						String line = "";
						FileSystem fs = FileSystem.get(conf);
						Path getFilePath = new Path(file.toString());
						BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
						while ((line = reader.readLine()) != null) {
							if (!line.endsWith(",,")) {
								String[] data = line.split(",");
								this.popData.put(data[1], data[4]);
							}
						}
					} catch (Exception e) {
						System.out.println("Unable to read the File");
						e.printStackTrace();
					}
				}
			}
		}

		public void reduce(Text country, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int sum = 0;
			double pop = 0.00;
			for (IntWritable value : values) {
				sum += value.get();
			}
			if (popData.containsKey(String.valueOf(country))) {
				pop = ((sum * 1.0) / (1.0 * Integer.parseInt(popData.get(String.valueOf(country))))) * 1000000;
				con.write(country, new Text(String.valueOf(pop)));
			}
		}
	}

}
