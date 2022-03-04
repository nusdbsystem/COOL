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

import java.nio.MappedByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author david
 *
 */
public class ChunkDimensionFactory {

	private ChunkColumnDictFactory dictFactory;

	private ChunkColumnIDsFactory idsFactory;

	public ChunkDimensionFactory(ChunkColumnDictFactory dictFactory,
			ChunkColumnIDsFactory idsFactory) {
		this.dictFactory = checkNotNull(dictFactory);
		this.idsFactory = checkNotNull(idsFactory);
	}

	public ChunkDimension newDimension(Codec dictCode, byte[] compressedDict,
									   Codec idsCode, byte[] compressedIDs) {
		ChunkColumnDict dict = dictFactory.newColumnDict(dictCode, compressedDict);
		ChunkColumnIDs ids = idsFactory.newColumnIDs(idsCode, compressedIDs);
		return new ChunkDimension(dictCode, dict, idsCode, ids);
	}

	public ChunkDimension load(MappedByteBuffer buffer, int numOfIDs) {
		ChunkColumnHeader header = ChunkColumnHeader.load(buffer);
		checkArgument(header.getType() == FieldType.Dimension);
		ChunkColumnDictHeader cHeader = ChunkColumnDictHeader.load(buffer);
		ChunkColumnDict dict = dictFactory.load(cHeader.codeType(), buffer);
		ChunkColumnIDsHeader idsHeader = ChunkColumnIDsHeader.load(buffer);
		ChunkColumnIDs ids = idsFactory.load(idsHeader.codeType(), buffer, numOfIDs);
		return new ChunkDimension(cHeader.codeType(), dict, idsHeader.codeType(),
				ids);
	}

}
