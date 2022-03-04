/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nus.cool.core.io;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.util.BitSet;

/**
 * BitVector storage for chunk column dictionary.
 * 
 *
 */
public class BitVectorChunkColumnDict implements ChunkColumnDict {

	private long[] words;

	private byte[] lookupTable;

	private int[] globalIDsArray = new int[0];

	private int numOfIDs;

	private BitVectorChunkColumnDict() {
	}

	public BitVectorChunkColumnDict(byte[] compressed) {
		// Convert byte order if necessary
		boolean bLittle = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
		if(bLittle) {
			this.words = BitSet.valueOf(ByteBuffer.wrap(compressed)).toLongArray();
		} else
			this.words = BitSet.valueOf(compressed).toLongArray();
		this.lookupTable = new byte[words.length];
		this.lookupTable[0] = 0;
		int len = words.length;
		for (int i = 1; i < len; i++) {
			lookupTable[i] = (byte) (Long.bitCount(words[i - 1]) + lookupTable[i - 1]);
		}
		numOfIDs = lookupTable[len - 1] + Long.bitCount(words[len - 1]);
	}

	public static ChunkColumnDict load(MappedByteBuffer buffer) {
		ChunkColumnDict dict = new BitVectorChunkColumnDict();
		dict.readFrom(buffer);
		return dict;
	}

	private final int wordIndex(int i) {
		return i >>> 6;
	}

	private final int remainder(int i) {
		return i & (64 - 1);
	}

	private boolean get(int bitIndex) {
		int i = wordIndex(bitIndex);
		return (i < words.length) && ((words[i] & (1L << bitIndex)) != 0);
	}

	@Override
	public void readFrom(ByteBuffer buffer) {
		int len = buffer.get() & 0xFF;
		this.words = new long[len];
		this.lookupTable = new byte[len];

		this.lookupTable[0] = 0;
		this.words[0] = buffer.getLong();

		for (int i = 1; i < len; i++) {
			words[i] = buffer.getLong();
			lookupTable[i] = (byte) (Long.bitCount(words[i - 1]) + lookupTable[i - 1]);
		}

		numOfIDs = lookupTable[len - 1] + Long.bitCount(words[len - 1]);

	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		boolean bLittle = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
		out.writeByte(words.length);
		for (long w : words)
			out.writeLong(bLittle ? Long.reverseBytes(w) : w);
	}

	private void fillInGlobalIDs() {
		BitSet bs = BitSet.valueOf(this.words);
		globalIDsArray = new int[numOfIDs];
		for (int i = bs.nextSetBit(0), j = 0; i >= 0; i = bs.nextSetBit(i + 1)) {
			globalIDsArray[j++] = i;
		}
	}

	@Override
	public int[] getGlobalIDs() {
		if (globalIDsArray.length == 0) {
			fillInGlobalIDs();
		}
		return globalIDsArray;
	}

	@Override
	public int getGlobalID(int localId) {
		if (globalIDsArray.length == 0) {
			fillInGlobalIDs();
		}
		return globalIDsArray[localId];
	}

	@Override
	public int getLocalID(int globalId) {
		int i = wordIndex(globalId);
		int j = remainder(globalId);
		long bits = words[i] << (63 - j);
		return Long.bitCount(bits) + (lookupTable[i] & 0xFF) - 1;
	}

	@Override
	public boolean contains(int... globalIDs) {
		for (int gID : globalIDs)
			if (get(gID))
				return true;
		return false;
	}

	@Override
	public int getNumOfIDs() {
		int last = lookupTable.length - 1;
		return (lookupTable[last] & 0xFF) + Long.bitCount(words[last]);
	}

	@Override
	public String toString() {
		return BitSet.valueOf(words).toString();
	}	
	
}
