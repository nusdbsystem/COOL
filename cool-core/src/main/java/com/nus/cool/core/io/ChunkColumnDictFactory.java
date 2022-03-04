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

public class ChunkColumnDictFactory {

	public ChunkColumnDict load(Codec codeType, MappedByteBuffer buffer) {
		ChunkColumnDict ccDict = null;
		switch (codeType) {
		case INT8:
		case INT16:
		case INT32:
			ccDict = ZIntChunkColumnDict.load(buffer, codeType);
			break;
		case BitVector:
			ccDict = BitVectorChunkColumnDict.load(buffer);
			break;
		default:
			throw new IllegalArgumentException("Unsupported CodeType: " + codeType);
		}
		return ccDict;
	}

	public ChunkColumnDict newColumnDict(Codec codeType, byte[] compressed) {
		ChunkColumnDict ccDict = null;
		switch (codeType) {
		case INT8:
		case INT16:
		case INT32:
			ccDict = new ZIntChunkColumnDict(codeType, compressed);
			break;
		case BitVector:
			ccDict = new BitVectorChunkColumnDict(compressed);
			break;
		default:
			throw new IllegalArgumentException("Unsupported CodeType: " + codeType);
		}
		return ccDict;
	}
}
