package com.tailtarget.bigdata.examples.crunch;

import java.net.MalformedURLException;
import java.net.URL;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

/**
 * This is an Apache Crunch doFn implementation that acts as one
 * step in the MapReduce pipeline.
 * It gets a web access log file, finds an url, extract the url domain and
 * counts how many visits that domain had.
 * See README file to learn how to execute this example.
 * 
 */
public class Listing5 extends DoFn<String, Pair<String, Integer>> {
    
    public Listing5() {
    }

    @Override
    public void process(String line, Emitter<Pair<String, Integer>> emitter) {

        String[] parts = line.split(" ");
        try {
            URL url = new URL(parts[2]);
            emitter.emit(Pair.of(url.getHost(), 1));
        } catch(MalformedURLException e) {
            // ignore
        }
    }
    
}
