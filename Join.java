package mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Join {
	public static int time = 0;

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String str = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(str);
			String[] values = new String[2];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				values[i] = tokenizer.nextToken();
				i++;
			}

			output.collect(new Text(values[1]), new Text("!" + values[0] + "\t"
					+ values[1]));
			output.collect(new Text(values[0]), new Text("#" + values[0] + "\t"
					+ values[1]));
		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] grandparent = new String[10];
			if (0 == time) {
				output.collect(new Text("Ëï×Ó"), new Text("×æ¸¸Ä¸"));
				time++;
			}
			String a = null;
			int b = 0;
			while (values.hasNext()) {
				String record = values.next().toString();
				if (record.startsWith("!")) {
					String str = record.replace("!", "");
					String[] value_1 = str.split("\t", 2);
					a = value_1[0];
				}

				if (record.startsWith("#")) {
					String str = record.replace("#", "");
					String[] value_1 = str.split("\t", 2);
					grandparent[b] = value_1[1];
					b++;
				}

			}

			if (a != null && 0 != b) {
				for (int n = 0; n < b; n++) {
					output.collect(new Text(a), new Text(grandparent[n]));
				}
			}
		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Join.class);
		conf.setJobName("WordCount");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		// conf.setPartitionerClass(MyPartitioner.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(1);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
