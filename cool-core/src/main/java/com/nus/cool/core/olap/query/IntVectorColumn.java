/**
 * 
 */
package com.nus.cool.core.olap.query;

/**
 * @author david
 *
 */
public class IntVectorColumn implements VectorColumn {

	@Override
	public int fetch(Object vector) {
		int[] toFill = (int[]) vector;
		//TODO: fill in the vector
		toFill[0] = 0;
		return 0;
	}

}
