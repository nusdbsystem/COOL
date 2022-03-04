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

import com.nus.cool.core.schema.Codec;

import java.nio.MappedByteBuffer;


public class ChunkColumnIDsFactory {

	public ChunkColumnIDs load(Codec codeType, MappedByteBuffer buffer,
							   int numOfIDs) {
		switch (codeType) {
		case INT8:
		case INT16:
		case INT32:
			return ZIntChunkColumnIDs.load(buffer, codeType, numOfIDs);
		case INTBIT:
			return ZIntBitChunkColumnIDs.load(buffer);
		case RLE:
			return RLEChunkColumnIDs.load(buffer);
		default:
			throw new IllegalArgumentException("CodeType " + codeType);
		}

	}

	public ChunkColumnIDs newColumnIDs(Codec codeType, byte[] compressed) {
		switch (codeType) {
		case INT8:
		case INT16:
		case INT32:
			return new ZIntChunkColumnIDs(codeType, compressed);
		case INTBIT:
			return new ZIntBitChunkColumnIDs(compressed);
		case RLE:
			return new RLEChunkColumnIDs(compressed);
		default:
			throw new IllegalArgumentException("CodeType " + codeType);
		}
	}
}
