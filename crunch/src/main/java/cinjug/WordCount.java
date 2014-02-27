package cinjug;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A word count example for Apache Crunch, based on Crunch's example projects.
 */
public class WordCount extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordCount(), args);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: hadoop jar ${artifactId}-${version}-job.jar"
									  + " [generic options] input output");
			System.err.println();
			GenericOptionsParser.printGenericCommandUsage(System.err);
			return 1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		Pipeline pipeline = new MRPipeline(WordCount.class, getConf());
		PCollection<String> lines = pipeline.readTextFile(inputPath);


		pipeline.writeTextFile(lines, outputPath);
		PipelineResult result = pipeline.done();

		return result.succeeded() ? 0 : 1;
	}

}
