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

import com.nus.cool.core.olap.schema.FieldType;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;


public abstract class ChunkColumn implements AeolusWritable {
	
	protected ChunkColumnHeader header;
		
	protected abstract void readInternal(ByteBuffer buffer);
	
	protected abstract void writeInternal(DataOutput out) throws IOException;
	
	// For write
	public ChunkColumn(FieldType type) {
		this.header = new ChunkColumnHeader(type);
	}
		
	public FieldType getType() {
		return header.getType();
	}

	@Override
	public void readFrom(ByteBuffer buffer) {
		this.header = ChunkColumnHeader.load((MappedByteBuffer) buffer);
		readInternal(buffer);
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		header.writeTo(out);
		writeInternal(out);
	}

}
