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

import com.google.common.base.MoreObjects;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Read/Write chunk header from/to data file
 * 
 * @author david
 *
 */
public class ChunkHeader implements AeolusWritable {

	private int numOfRecords;

	private IntBuffer columnOffsets;

	private int numOfOffsets = 0;

	private ChunkHeader() {
	}

	public ChunkHeader(int numOfRecords, int[] offsets) {
		checkArgument(numOfRecords > 0);
		this.numOfRecords = numOfRecords;
		this.columnOffsets = IntBuffer.wrap(checkNotNull(offsets));
		this.numOfOffsets = offsets.length;
	}

	public static ChunkHeader load(MappedByteBuffer buffer) {
		ChunkHeader cHeader = new ChunkHeader();
		cHeader.readFrom(buffer);
		return cHeader;
	}

	public int getColumnOffset(int i) {
		return columnOffsets.get(i);
	}

	public int getNumOfRecords() {
		return numOfRecords;
	}

	@Override
	public void readFrom(ByteBuffer buf) {
		this.numOfRecords = buf.getInt();
		this.numOfOffsets = buf.getShort() & 0xFFFF;
		int len = 4 * numOfOffsets;
		int oldLimit = buf.limit();
		int newLimit = buf.position() + len;
		buf.limit(newLimit);
		this.columnOffsets = buf.asIntBuffer();
		buf.position(newLimit);
		buf.limit(oldLimit);
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		boolean bLittle = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
		int n0 = bLittle ? Integer.reverseBytes(numOfRecords) : numOfRecords;
		short n1 = (short) numOfOffsets;
		n1 = bLittle ? Short.reverseBytes(n1) : n1;
		out.writeInt(n0);
		out.writeShort(n1);
		while (columnOffsets.hasRemaining()) {
			int off = columnOffsets.get();
			off = bLittle ? Integer.reverseBytes(off) : off;
			out.writeInt(off);
		}
	}

	@Override
	public String toString() {
		int[] offset = new int[columnOffsets.limit()];
		for (int i = 0; i < offset.length; i++) 
			offset[i] = columnOffsets.get(i);
		//columnOffsets.get(offset);
		return MoreObjects.toStringHelper(this)
				.add("Records", numOfRecords)
				.add("ColumnOffsets", Arrays.toString(offset))
				.toString();
	}
}
