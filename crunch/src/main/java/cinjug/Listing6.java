package com.tailtarget.bigdata.examples.crunch;

import java.io.File;
import java.io.Serializable;
import org.apache.commons.io.FileUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.plan.PlanningParameters;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class implements an Apache Crunch pipeline.
 * It gets a web access log file, finds an url, extract the url domain and
 * counts how many visits that domain had.
 * See README file to learn how to execute this example.
 * 
 */
public class Listing6 extends Configured implements Tool, Serializable {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set("fs.default.name", args[0]);
        config.set("mapred.map.tasks.speculative.execution", "false");
        config.set("mapred.reduce.tasks.speculative.execution", "false");
        config.set(RuntimeParameters.TMP_DIR, "/tmp/demos-pipeline/");
        
        int result = ToolRunner.run(config, new Listing6(), args);
        if (result != 0) {
            System.err.println("Hadoop process finished with error. ");
            System.exit(1);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {

        // Finds logs that need processing
        FileSystem fs = FileSystem.get(getConf());
        Path path = new Path(args[1]);
        FileStatus[] files = fs.listStatus(path);
       
        if (files != null && files.length > 0) {
            Pipeline pipeline = new MRPipeline(Listing6.class, getConf());
            pipeline.enableDebug(); 

            // Process log files and counts how many times a domain was accessed
            for (FileStatus status : files) {
                PCollection<String> lines = pipeline.readTextFile(status.getPath().toString());

                PTable<String, Integer> visitors = lines.parallelDo("Listing6", 
                               new Listing5(),  
                               Writables.tableOf(Writables.strings(), Writables.ints()));
                                           
                PGroupedTable<String, Integer> grouped = visitors.groupByKey(1);
                
                PTable<String, Integer> counts = grouped.combineValues(Aggregators.SUM_INTS());

                // We are simply saving the output to /tmp/demos, so you can check the results
                // You can adapt this to your own evironment
                pipeline.writeTextFile(counts, "/tmp/demos/crunch"+System.nanoTime());
                

            }
            
            PipelineResult pipelineResult = pipeline.done();
            
            // Saves a dot file on /tmp to show a graph of how the pipeline was organized
            String dotFileContents = pipeline.getConfiguration().get(PlanningParameters.PIPELINE_PLAN_DOTFILE);
            FileUtils.writeStringToFile(new File("/tmp/logpipelinegraph.dot"), dotFileContents);

            return (pipelineResult == null || pipelineResult.succeeded()) ? 0 : 1;
        } 
        return 0;
    }
    
}
