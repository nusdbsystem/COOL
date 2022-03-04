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


import com.nus.cool.core.olap.schema.FieldType;
import com.nus.cool.core.schema.Codec;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 */
public class ChunkDimension extends ChunkColumn {

	private ChunkColumnDictHeader dictHeader;

	private ChunkColumnDict dict;

	private ChunkColumnIDsHeader idHeader;

	private ChunkColumnIDs ids;
	
	// For write
	public ChunkDimension(Codec dictCodec, ChunkColumnDict dict, Codec idCodec,
			ChunkColumnIDs ids) {
		super(FieldType.Dimension);
		this.dictHeader = new ChunkColumnDictHeader(dictCodec);
		this.dict = checkNotNull(dict);
		this.idHeader = new ChunkColumnIDsHeader(idCodec);
		this.ids = checkNotNull(ids);
	}

	public ChunkColumnDict getDict() {
		return this.dict;
	}

	public ChunkColumnIDs getIDs() {
		return this.ids;
	}

	@Override
	protected void readInternal(ByteBuffer buffer) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void writeInternal(DataOutput out) throws IOException {
		dictHeader.writeTo(out);
		dict.writeTo(out);
		idHeader.writeTo(out);
		ids.writeTo(out);
	}

}
