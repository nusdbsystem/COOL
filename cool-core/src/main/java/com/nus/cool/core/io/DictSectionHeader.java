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

import com.google.common.primitives.Ints;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;


public class DictSectionHeader implements AeolusWritable {

	private int numOfColumns;

	private IntBuffer offsets;

	private DictSectionHeader() {
	}

	public DictSectionHeader(int numOfColumns, int[] offsets) {
		checkArgument(numOfColumns > 0);
		checkArgument(offsets != null && offsets.length > 0);
		this.numOfColumns = numOfColumns;
		this.offsets = ByteBuffer.allocate(offsets.length * Ints.BYTES)
				.order(ByteOrder.nativeOrder()).asIntBuffer();
		this.offsets.put(offsets).flip();
	}

	public static DictSectionHeader load(MappedByteBuffer buffer) {
		DictSectionHeader header = new DictSectionHeader();
		header.readFrom(buffer);
		return header;
	}

	public int getFringerOffset(int i) {
		return offsets.get(i * 2);
	}

	public int getTermOffset(int i) {
		return offsets.get(i * 2 + 1);
	}

	@Override
	public void readFrom(ByteBuffer buffer) {
		this.numOfColumns = buffer.getShort() & 0xFFFF;
		int length = numOfColumns * 8;
		int oldLimit = buffer.limit();
		int newLimit = buffer.position() + length;
		buffer.limit(newLimit);
		this.offsets = buffer.asIntBuffer();
		buffer.position(newLimit);
		buffer.limit(oldLimit);		
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		boolean bLittle = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
		short n0 = (short) numOfColumns;
		out.writeShort(bLittle ? Short.reverseBytes(n0) : n0);
		while (offsets.hasRemaining()) {
			int off = offsets.get();
			out.writeInt(bLittle ? Integer.reverseBytes(off) : off);
		}
	}

}
