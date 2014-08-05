/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.examples.workload;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Comments here
 */
public class WorkGen extends Configured implements Tool {

	static int printUsage() {
		System.out.println("sort [-m <maps>] [-r <reduces> has no effect] " + "[-inFormat <input format class>] " + "[-outFormat <output format class>] " + "[-outKey <output key class>] "
				+ "[-outValue <output value class>] " + "<input> <output> <shuffleInputRatio> <outputShuffleRatio>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * User counters
	 */
	static enum Counters {
		MAP_RECORDS_WRITTEN, MAP_BYTES_WRITTEN, RED_RECORDS_WRITTEN, RED_BYTES_WRITTEN
	};

	/**
	 * Comments
	 */
	static class RatioMapper extends Mapper<WritableComparable, Writable, BytesWritable, BytesWritable> {

		private double shuffleInputRatio = 1.0d;

		private int minKeySize;
		private int keySizeRange;
		private int minValueSize;
		private int valueSizeRange;
		private Random random = new Random();
		private BytesWritable randomKey;
		private BytesWritable randomValue;

		private void randomizeBytes(byte[] data, int offset, int length) {
			for (int i = offset + length - 1; i >= offset; --i) {
				data[i] = (byte) random.nextInt(256);
			}
		}

		@Override
		protected void map(WritableComparable key, Writable value, Context context) throws IOException, InterruptedException {
			double shuffleInputRatioTemp = shuffleInputRatio;

			// output floor(shuffleInputRatio) number of intermediate pairs
			while (shuffleInputRatioTemp >= 0.0d) {
				int keyLength = minKeySize + (keySizeRange != 0 ? random.nextInt(keySizeRange) : 0);
				randomKey = new BytesWritable();
				randomKey.setSize(keyLength);
				randomizeBytes(randomKey.getBytes(), 0, randomKey.getLength());
				int valueLength = minValueSize + (valueSizeRange != 0 ? random.nextInt(valueSizeRange) : 0);
				randomValue = new BytesWritable();
				randomValue.setSize(valueLength);
				randomizeBytes(randomValue.getBytes(), 0, randomValue.getLength());
				if (shuffleInputRatioTemp >= 1.0d || (random.nextDouble() < shuffleInputRatioTemp)) {
					context.write(randomKey, randomValue);
					context.getCounter(Counters.MAP_BYTES_WRITTEN).increment(keyLength + valueLength);
					context.getCounter(Counters.MAP_RECORDS_WRITTEN).increment(1);
				}
				shuffleInputRatioTemp -= 1.0d;
			} // end while
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			shuffleInputRatio = Double.parseDouble(conf.getRaw("workGen.ratios.shuffleInputRatio"));
			minKeySize = conf.getInt("workGen.randomwrite.min_key", 10);
			keySizeRange = conf.getInt("workGen.randomwrite.max_key", 1000) - minKeySize;
			minValueSize = conf.getInt("workGen.randomwrite.min_value", 0);
			valueSizeRange = conf.getInt("workGen.randomwrite.max_value", 20000) - minValueSize;

		}

	} // end static class RatioMapper

	/**
	 * Comments
	 */
	static class RatioReducer extends Reducer<WritableComparable, Writable, BytesWritable, BytesWritable> {

		private double outputShuffleRatio = 1.0d;

		private int minKeySize;
		private int keySizeRange;
		private int minValueSize;
		private int valueSizeRange;
		private Random random = new Random();
		private BytesWritable randomKey;
		private BytesWritable randomValue;

		private void randomizeBytes(byte[] data, int offset, int length) {
			for (int i = offset + length - 1; i >= offset; --i) {
				data[i] = (byte) random.nextInt(256);
			}
		}

		@Override
		protected void reduce(WritableComparable key, Iterable<Writable> values, Context context) throws IOException, InterruptedException {
			Iterator<Writable> it = values.iterator();
			while (it.hasNext()) {
				Writable value = it.next();

				double outputShuffleRatioTemp = outputShuffleRatio;

				// output floor(outputShuffleRatio) number of intermediate pairs
				while (outputShuffleRatioTemp >= 0.0d) {
					int keyLength = minKeySize + (keySizeRange != 0 ? random.nextInt(keySizeRange) : 0);
					randomKey = new BytesWritable();
					randomKey.setSize(keyLength);
					randomizeBytes(randomKey.getBytes(), 0, randomKey.getLength());
					int valueLength = minValueSize + (valueSizeRange != 0 ? random.nextInt(valueSizeRange) : 0);
					randomValue = new BytesWritable();
					randomValue.setSize(valueLength);
					randomizeBytes(randomValue.getBytes(), 0, randomValue.getLength());
					if (outputShuffleRatioTemp >= 1.0d || (random.nextDouble() < outputShuffleRatioTemp)) {
						context.write(randomKey, randomValue);
						context.getCounter(Counters.RED_BYTES_WRITTEN).increment(keyLength + valueLength);
						context.getCounter(Counters.RED_RECORDS_WRITTEN).increment(1);
					}
					outputShuffleRatioTemp -= 1.0d;
				} // end while
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			outputShuffleRatio = Double.parseDouble(conf.getRaw("workGen.ratios.outputShuffleRatio"));
			minKeySize = conf.getInt("workGen.randomwrite.min_key", 10);
			keySizeRange = conf.getInt("workGen.randomwrite.max_key", 10) - minKeySize;
			minValueSize = conf.getInt("workGen.randomwrite.min_value", 90);
			valueSizeRange = conf.getInt("workGen.randomwrite.max_value", 90) - minValueSize;

		}

	}

	/**
	 * The main driver for the program. Invoke this method to submit the
	 * map/reduce job.
	 * 
	 * @throws IOException
	 *             When there is communication problems with the job tracker.
	 */
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), WorkGen.class.getCanonicalName());
		job.setJarByClass(WorkGen.class);
		job.setMapperClass(RatioMapper.class);
		job.setReducerClass(RatioReducer.class);

		int num_maps = 16;
		int num_reduces = 1;

		String sort_reduces = job.getConfiguration().get("workGen.sort.reduces_per_host");
		if (sort_reduces != null) {
			num_reduces = 6 * Integer.parseInt(sort_reduces);
		}

		Class<? extends InputFormat> inputFormatClass = SequenceFileInputFormat.class;
		Class<? extends OutputFormat> outputFormatClass = SequenceFileOutputFormat.class;
		Class<? extends WritableComparable> outputKeyClass = BytesWritable.class;
		Class<? extends Writable> outputValueClass = BytesWritable.class;

		List<String> otherArgs = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					num_maps = Integer.parseInt(args[++i]);
				} else if ("-r".equals(args[i])) {
					num_reduces = Integer.parseInt(args[++i]);
				} else if ("-inFormat".equals(args[i])) {
					inputFormatClass = Class.forName(args[++i]).asSubclass(InputFormat.class);
				} else if ("-outFormat".equals(args[i])) {
					outputFormatClass = Class.forName(args[++i]).asSubclass(OutputFormat.class);
				} else if ("-outKey".equals(args[i])) {
					outputKeyClass = Class.forName(args[++i]).asSubclass(WritableComparable.class);
				} else if ("-outValue".equals(args[i])) {
					outputValueClass = Class.forName(args[++i]).asSubclass(Writable.class);
				} else {
					otherArgs.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
				return printUsage(); // exits
			}
		}

		// Set user-supplied (possibly default) job configs
		job.setNumReduceTasks(num_reduces);

		job.setInputFormatClass(inputFormatClass);
		job.setOutputFormatClass(outputFormatClass);

		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);

		// Make sure there are exactly 4 parameters left.
		if (otherArgs.size() != 5) {
			System.out.println("ERROR: Wrong number of parameters: " + otherArgs.size() + " instead of 4.");
			return printUsage();
		}
		String jobName = otherArgs.get(0);
		job.setJobName(jobName);
		BufferedReader input = new BufferedReader(new FileReader(otherArgs.get(1)));
		String inputPaths = input.readLine();
		FileInputFormat.setInputPaths(job, inputPaths);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(2)));
		job.getConfiguration().set("workGen.ratios.shuffleInputRatio", otherArgs.get(3));
		job.getConfiguration().set("workGen.ratios.outputShuffleRatio", otherArgs.get(4));

		System.out.println("shuffleInputRatio  = " + Double.parseDouble(job.getConfiguration().getRaw("workGen.ratios.shuffleInputRatio")));
		System.out.println("outputShuffleRatio = " + Double.parseDouble(job.getConfiguration().getRaw("workGen.ratios.outputShuffleRatio")));

		Date startTime = new Date();
		Random random = new Random();
		System.out.println(random.nextDouble());
		System.out.println(random.nextDouble());
		System.out.println("Job started: " + startTime);
		job.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) / 1000 + " seconds.");
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WorkGen(), args);
		System.exit(res);
	}

}
