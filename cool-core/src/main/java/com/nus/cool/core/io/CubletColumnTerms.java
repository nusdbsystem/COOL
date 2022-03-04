/**
 * 
 */
package com.nus.cool.core.io;

import java.nio.charset.Charset;

/**
 * Terms of a cublet column
 * 
 * Allowed access method: sequential, random
 * 
 */
public interface CubletColumnTerms extends AeolusWritable {
	
	String getTerm(int termID, Charset charset);

	public int terms();
	
}
