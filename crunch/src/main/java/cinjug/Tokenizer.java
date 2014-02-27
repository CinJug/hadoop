package cinjug;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

/**
 * Splits a line of text, filtering known stop words.
 */
public class Tokenizer extends DoFn<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	@Override
	public void process(String line, Emitter<String> emitter) {
	}

}
