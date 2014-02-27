package cinjug;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and
	 * a mapper and a reducer working together.
	 */
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		WordMapper mapper = new WordMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		WordReducer reducer = new WordReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("cinjug cinjug cinjug hadoop mapred yarn cinjug"));
		mapDriver.withOutput(new Text("cinjug"), new IntWritable(1));
		mapDriver.withOutput(new Text("cinjug"), new IntWritable(1));
		mapDriver.withOutput(new Text("cinjug"), new IntWritable(1));
		mapDriver.withOutput(new Text("hadoop"), new IntWritable(1));
		mapDriver.withOutput(new Text("mapred"), new IntWritable(1));
		mapDriver.withOutput(new Text("yarn"), new IntWritable(1));
		mapDriver.withOutput(new Text("cinjug"), new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("cinjug"), values);
		reduceDriver.withOutput(new Text("cinjug"), new IntWritable(2));
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new LongWritable(1), new Text("cinjug cinjug cinjug"));
		mapReduceDriver.addOutput(new Text("cinjug"), new IntWritable(3));
		
		mapReduceDriver.runTest();
	}
	
}
