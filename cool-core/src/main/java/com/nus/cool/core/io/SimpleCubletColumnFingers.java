/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.nus.cool.core.io;

import com.nus.cool.core.lang.Integers;
import com.nus.cool.core.util.IntBuffers;
import com.rabinhash.RabinHashFunction32;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;


public class SimpleCubletColumnFingers implements CubletColumnFingers {
	
	private final RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;
	
	private IntBuffer fp;
	
	private SimpleCubletColumnFingers(){}
	
	public SimpleCubletColumnFingers(byte[] vals, int[] offsets){
		int[] fps = new int[offsets.length];		
		int i;
		
		for (i = 0; i < offsets.length - 1; i++) {			
			fps[i] = rhash.hash(Arrays.copyOfRange(vals, offsets[i], offsets[i+1]));						
		}		
		// last dict entry
		fps[i] = rhash.hash(Arrays.copyOfRange(vals, offsets[i], vals.length));
		
		Arrays.sort(fps);
		
		this.fp = IntBuffer.wrap(fps);
	}
	
	public static CubletColumnFingers load(MappedByteBuffer buffer) {
		CubletColumnFingers fp = new SimpleCubletColumnFingers();
		fp.readFrom(buffer);
		return fp;
	}


	@Override
	public void readFrom(ByteBuffer buffer) {
		int numOfEntry = buffer.getInt();
		int oldLimit = buffer.limit();
		int newPosition = buffer.position() + 4 * numOfEntry;
		
		buffer.limit(newPosition);
		this.fp = buffer.asIntBuffer();
		buffer.position(newPosition);
		buffer.limit(oldLimit);
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		out.writeInt(Integers.toNativeByteOrder(fp.limit()));
		
		fp.rewind();
		while (fp.hasRemaining())
			out.writeInt(Integers.toNativeByteOrder(fp.get()));
	}

	@Override
	public int getTermID(String term) {		
		return IntBuffers.binarySearch(this.fp, rhash.hash(term));
	}

	@Override
	public int fingers() {
		// TODO Auto-generated method stub
		return 0;
	}

}
