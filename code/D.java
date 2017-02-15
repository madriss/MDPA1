package edu.stanford.cs246.invertedindex;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ToolRunner;

import edu.stanford.cs246.invertedindex.Aii.Reduce;


public class D extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new D(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "Unique words inverted index");

		job.setJarByClass(D.class);
		job.setOutputKeyClass(Text.class); // we set the output key to a text (word)
		job.setOutputValueClass(Text.class); // we set the output value to a text (text file name)
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combiner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " -> "); //for csv files
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0])); // reading input argument
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // reading output argument

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(new Path(args[1]))) { // if output already exists we delete it
			fs.delete(new Path(args[1]), true);
		}

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text filename = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			HashSet<String> stopwords = new HashSet<String>();
			BufferedReader Reader = new BufferedReader(new FileReader(new File("/home/cloudera/Desktop/Assignments/stopwords.csv"))); // we use a small python script to get only the stop words without their number of occurences
			String A;
			while ((A = Reader.readLine()) != null) {
				stopwords.add(A.toLowerCase());
			}

			String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName(); //getting the name of the file
			filename = new Text(filenameStr);

			for (String token : value.toString().split("\\s+")) {
				if (!stopwords.contains(token.toLowerCase())) {
					word.set(token.toLowerCase()); //setting words to lower case to avoid duplicates
					context.write(word, filename);
				}
			}
			
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String> set = new HashSet<String>(); // a hashet is a data structure that only stores unique occurences (no duplicates)
			for (Text value : values) {
				set.add(value.toString());
			}
			StringBuilder stringbuilder = new StringBuilder();
			for (String value : set) {
				stringbuilder.append(value);
				stringbuilder.append(", ");
			}
			context.write(key, new Text(stringbuilder.toString()));
			}
		}
	
	 public static class Combiner extends Reducer<Text, Text, Text, Text> {
	      @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	    	 HashMap<String,Integer> result = new HashMap<String,Integer>();
	         for (Text value : values) {
	        	 if(result.containsKey(value.toString())){
	        		 result.put(value.toString(), result.get(value.toString())+1); 
	        	 }
	        	 else{
	        		 result.put(value.toString(), 1) ; 
	        	 } 
	         }
	         context.write(key, new Text(result.toString().replace("=", "#").replace("}", "").replace("{", "")));
	      }
	   }
}
