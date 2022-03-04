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

import com.google.common.base.MoreObjects;
import com.nus.cool.core.schema.Codec;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Header of a chunk column dictionary.
 * 
 *
 */
public class ChunkColumnDictHeader implements AeolusWritable {
	
	private Codec codeType;
	
	private ChunkColumnDictHeader() { }
	
	public ChunkColumnDictHeader(Codec type) {
		this.codeType = type;
	}
	
	public static ChunkColumnDictHeader load(MappedByteBuffer buffer) {
		ChunkColumnDictHeader header = new ChunkColumnDictHeader();
		header.readFrom(buffer);
		return header;
	}
	
	public Codec codeType() {
		return this.codeType;
	}
	
	@Override
	public void readFrom(ByteBuffer buffer) {
		this.codeType = Codec.fromInteger(buffer.get() & 0xFF);
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		out.writeByte(codeType.ordinal());
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("CodingType", codeType)
				.toString();
	}
	
}
