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
import com.nus.cool.core.olap.schema.FieldType;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class ChunkColumnHeader implements AeolusWritable {
	
	private FieldType type;
	
	private ChunkColumnHeader() {}
	
	public ChunkColumnHeader(FieldType type) {
		this.type = type;
	}
	
	public static ChunkColumnHeader load(MappedByteBuffer buffer) {
		ChunkColumnHeader header = new ChunkColumnHeader();
		header.readFrom(buffer);
		return header;
	}

	public FieldType getType() {
		return type;
	}

	@Override
	public void readFrom(ByteBuffer buffer) {
		this.type = FieldType.fromInteger(buffer.get() & 0xFF);
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		out.writeByte(type.ordinal());
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("ColumnType", type)
				.toString();
	}

}
