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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Read/Write header from/to cublet data file
 * 
 *
 */
public class DataSectionHeader implements AeolusWritable {
	
	private int numOfChunks;
	
	private int numOfColumns;
	
	private IntBuffer chunkOffsets;
	
	private DataSectionHeader() { }
	
	public DataSectionHeader(int numOfChunks, int numOfColumns, int[] offsets) {
		checkArgument(numOfChunks > 0);
		checkArgument(numOfColumns > 0);
		this.numOfChunks = numOfChunks;
		this.numOfColumns = numOfColumns;
		this.chunkOffsets = IntBuffer.wrap(checkNotNull(offsets));
		checkArgument(numOfChunks == offsets.length);
	}
	
	public static DataSectionHeader load(MappedByteBuffer buffer) {
		DataSectionHeader header = new DataSectionHeader();
		header.readFrom(buffer);
		return header;
	}
	
	public int getNumOfChunks() {
		return this.numOfChunks;
	}
		
	public int getNumOfColumns() {
		return this.numOfColumns;
	}
		
	public int getChunkOffset(int i) {
		return chunkOffsets.get(i);
	}
	
	@Override
	public void readFrom(ByteBuffer buffer) {
		buffer.order(ByteOrder.nativeOrder());
		this.numOfColumns = (buffer.getShort() & 0xFFFF);
		this.numOfChunks = (buffer.getShort() & 0xFFFF);
		int length = numOfChunks * 4;
		int oldLimit = buffer.limit();
		int newLimit = buffer.position() + length;
		buffer.limit(newLimit);
		this.chunkOffsets = buffer.asIntBuffer();
		buffer.position(newLimit);
		buffer.limit(oldLimit);
	}
	
	@Override
	public void writeTo(DataOutput out) throws IOException {
		boolean bLittle = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
		short n0 = (short) numOfColumns;
		short n1 = (short) numOfChunks;
		n0 = bLittle ? Short.reverseBytes(n0) : n0;
		n1 = bLittle ? Short.reverseBytes(n1) : n1;
		out.writeShort(n0);
		out.writeShort(n1);
		while(chunkOffsets.hasRemaining()) {
			int coff = chunkOffsets.get();
			coff = bLittle ? Integer.reverseBytes(coff) : coff;
			out.writeInt(coff);
		}
	}

	@Override
	public String toString() {
		String offsets = "";
		for (int i = 0; i < chunkOffsets.limit(); i++)
			offsets += chunkOffsets.get(i) + ", ";
		
		return MoreObjects.toStringHelper(this)
				.add("Columns", numOfColumns)
				.add("Chunks", numOfChunks)
				.add("ChunkOffsets", offsets)
				.toString();
	}
	
}
