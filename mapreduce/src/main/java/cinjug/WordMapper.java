package cinjug;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
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

	public void configure(Configuration conf) {
		caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);

		if (conf.getBoolean("wordcount.skip.patterns", false)) {
			Path[] patternsFiles = new Path[0];
			try {
				patternsFiles = DistributedCache.getLocalCacheFiles(conf);
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files: " + ioe.getMessage());
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
			context.write(word, one);
			context.getCounter(Counters.INPUT_WORDS).increment(1);
		}

		if ((++numRecords % 100) == 0) {
			context.setStatus("Finished processing " + numRecords + " records " + "from the input file: " 
					+ context.getInputSplit());
		}
	}
	
	private void parseSkipFile(Path patternsFile) {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(patternsFile.toString()));
			String pattern = null;
			while ((pattern = in.readLine()) != null) {
				patternsToSkip.add(pattern);
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file '" + patternsFile 
					+ "' : " + ioe.getMessage());
		} finally {
			IOUtils.closeQuietly(in);
		}
	}
	
}
