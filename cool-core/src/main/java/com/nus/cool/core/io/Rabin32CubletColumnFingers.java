/*
 * Copyright 2020 Cool Squad Team
 *
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

import com.google.common.primitives.Ints;
import com.nus.cool.core.util.IntBuffers;
import com.rabinhash.RabinHashFunction32;


import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 */
public class Rabin32CubletColumnFingers implements CubletColumnFingers {

	private RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;

	private IntBuffer fingers;

	private Rabin32CubletColumnFingers() {}
	
	public Rabin32CubletColumnFingers(int[] fps) {
		checkNotNull(fps);
		this.fingers = ByteBuffer.allocate(fps.length * Ints.BYTES)
				.order(ByteOrder.nativeOrder()).asIntBuffer().put(fps);
		this.fingers.flip();
	}
	
	public static CubletColumnFingers load(ByteBuffer buffer) {
		CubletColumnFingers fingers = new Rabin32CubletColumnFingers();
		fingers.readFrom(buffer);
		return fingers;
	}

	@Override
	public void readFrom(ByteBuffer buffer) {
		int values = buffer.getInt();
		int lengthInBytes = values * Ints.BYTES;
		int oldLimit = buffer.limit();
		int newLimit = buffer.position() + lengthInBytes;
		buffer.limit(newLimit);
		this.fingers = buffer.asIntBuffer();
		buffer.position(newLimit);
		buffer.limit(oldLimit);
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		boolean bLittle = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
		int size = fingers.limit();
		out.writeInt(bLittle ? Integer.reverseBytes(size) : size);
		while (fingers.hasRemaining()) {
			int tmp = fingers.get();
			out.writeInt(bLittle ? Integer.reverseBytes(tmp) : tmp);
		}
	}

	@Override
	public int getTermID(String term) {
		int fp = rhash.hash(term);
		return IntBuffers.binarySearch(fingers, 0, fingers.limit(), fp);
	}

	@Override
	public int fingers() {
		return fingers.limit();
	}

}
