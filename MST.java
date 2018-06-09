import java.io.IOException;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.log4j.Logger;

import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.Counters;


public class MST extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(MST.class);
	private static final Pattern delimiter = Pattern.compile(",");

	static enum MSTCounters { totalWeight }

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new MST(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "MST");
		job.setJarByClass(this.getClass());	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MSTMapper.class);
		job.setReducerClass(MSTReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		if (args[2].equals("max")) {
			job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		}
		else if (args[2].equals("min")) { }
		else {
			System.out.println("Invalid spanning tree type, must be either min or max. Aborting...");
			System.exit(1);
		}
		int completed = job.waitForCompletion(true) ? 0 : 1;

		Counters jobCounters = job.getCounters();
		long totalWeight = jobCounters.findCounter(MSTCounters.totalWeight).getValue();
		System.out.println("The total weight of the " + args[2] + "ST is " + totalWeight);
		return completed;
	}

	public static class MSTMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		public void map(LongWritable key, Text textLine, Context context) throws IOException, InterruptedException {
			String[] line = delimiter.split(textLine.toString());
			Text edge = new Text(line[0] + "," + line[1]);
			LongWritable weight = new LongWritable(Integer.parseInt(line[2]));
			context.write(weight, edge);
		}
	}

	public static class MSTReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		Map<String, Set<String>> forest = new HashMap<String, Set<String>>();

		public void reduce(LongWritable weight, Iterable<Text> edges, Context context) throws IOException, InterruptedException {
			long edgeWeight;
			for (Text edge : edges) {
				String[] nodes = delimiter.split(edge.toString());
				Set<String> set1 = new HashSet<String>();
				Set<String> set2 = new HashSet<String>();
				boolean condition1 = inSameSet(nodes[0], nodes[1]);
				set1.add(nodes[0]);
				set1.add(nodes[1]);
				boolean condition2 = unionFind(nodes[0], nodes[1], set1);
				set2.add(nodes[0]);
				set2.add(nodes[1]);
				boolean condition3 = unionFind(nodes[1], nodes[0], set2);
				if (!condition1 && !condition2 && !condition3) {
					edgeWeight = Long.parseLong(weight.toString());
					context.getCounter(MSTCounters.totalWeight).increment(edgeWeight);
					context.write(weight, edge);
				}
			}
		}

		public boolean unionFind(String source, String destination, Set<String> set) {
			boolean belongToSameSet = false;
			if (!forest.containsKey(source)) {
				forest.put(source, set);
			}
			else {
				Set<String> sourceSet = forest.get(source);
				Set<String> newSet = new HashSet<String>();
				newSet.addAll(sourceSet);
				Iterator<String> newSetIter1 = newSet.iterator();
				Iterator<String> newSetIter2 = newSet.iterator();

				while (newSetIter1.hasNext()) {
					String node = newSetIter1.next();
					if (forest.get(node).contains(destination)) {
						belongToSameSet = true;
						break;
					}
				}

				while (newSetIter2.hasNext()) {
					String node = newSetIter2.next();
					if (!forest.containsKey(node)) {
						forest.put(node, set);
					}
					forest.get(node).addAll(set);
				}
			}
			return belongToSameSet;
		}

		public boolean inSameSet(String source, String destination) {
			if (forest.get(source) != null && forest.get(destination) != null) {
				if (forest.get(source).contains(destination) && forest.get(destination).contains(source)) {
					return true;
				}
				else {
					return false;
				}
			}
			else {
				return false;
			}
		}
	}
}