
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ex2 {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Collection<String> has = new HashSet<String>();
			int num = 0;
			String[] item = value.toString().split("\t");
			String ip = item[0];
			has.add(ip);
			num = has.size();
			context.write(new Text(), new IntWritable(num));
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MapReduce");
		job.setJarByClass(Ex2.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
