package cinjug;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	static enum Counters { INPUT_WORDS }

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	private boolean caseSensitive = true;
	private Set<String> patternsToSkip = new HashSet<String>();

	private long numRecords = 0;
	private String inputFile;

	public void configure(JobConf job) {
		caseSensitive = job.getBoolean("wordcount.case.sensitive", true);
		inputFile = job.get("map.input.file");

		if (job.getBoolean("wordcount.skip.patterns", false)) {
			Path[] patternsFiles = new Path[0];
			try {
				patternsFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
			}
			for (Path patternsFile : patternsFiles) {
				parseSkipFile(patternsFile);
			}
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

		for (String pattern : patternsToSkip) {
			line = line.replaceAll(pattern, "");
		}

		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			output.collect(word, one);
			reporter.incrCounter(Counters.INPUT_WORDS, 1);
		}

		if ((++numRecords % 100) == 0) {
			reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
		}
	}
	
}
