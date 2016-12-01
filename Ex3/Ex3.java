
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

public class Ex3 {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		private HashSet<String> hs = new HashSet<String>();
		@SuppressWarnings("resource")
		public void setup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			FileSystem file = FileSystem.get(conf);
			Path dfsPath = new Path("/user/hadoop/mapred_dev_double/ip_time");
			FSDataInputStream fsIs = file.open(dfsPath);
			Scanner scanner = new Scanner(fsIs);
			while (scanner.hasNext()){
				String line = scanner.nextLine();
				hs.add(line);
			}
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] item = value.toString().split("\t");
			String strIP = item[0];
			if (hs.contains(strIP)){
				context.write(new Text(strIP), new IntWritable(1));
			}
		}
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
		}
		public void cleanup(Context context) throws IOException, InterruptedException{
			context.write(new Text(), result);
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MapReduce");
		job.setJarByClass(Ex3.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
