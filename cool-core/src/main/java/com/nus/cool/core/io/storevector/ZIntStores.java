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
package com.nus.cool.core.io.storevector;


import com.nus.cool.core.lang.Integers;
import com.nus.cool.core.schema.Codec;

import java.nio.ByteBuffer;

public class ZIntStores {

	public static Codec getCodeType(int value) {
		int numOfByte = Integers.minBytes(value);
		switch (numOfByte) {
		case 1:
			return Codec.INT8;
		case 2:
			return Codec.INT16;
		case 3:
		case 4:
			return Codec.INT32;
		default:
			throw new IllegalArgumentException("two many bytes");
		}
	}

//	public static Store newStore(CodeType codeType, int numOfValues) {
//		Store store;
//		switch (codeType) {
//		case ByteAlign:
//			store = new ByteAlignedStore(numOfValues);
//			break;
//		case ShortAlign:
//			store = new ShortAlignedStore(numOfValues);
//			break;
//		case IntAlign:
//			store = new IntAlignedStore(numOfValues);
//			break;
//		default:
//			throw new IllegalArgumentException("Not BytesAlignedStore " + codeType);
//		}
//		return store;
//	}

//	public static Store newStore(CodeType codeType, byte[] data) {
//		Store store;
//		switch (codeType) {
//		case ByteAlign:
//			store = new ByteAlignedStore(data);
//			break;
//		case ShortAlign:
//			store = new ShortAlignedStore(data);
//			break;
//		case IntAlign:
//			store = new IntAlignedStore(data);
//			break;
//		default:
//			throw new IllegalArgumentException("Not BytesAlignedStore " + codeType);
//		}
//		return store;
//	}
	
	public static ZIntStore newStore(Codec codeType, byte[] data) {
//		int values = -1;
//		switch (codeType) {
//		case INT8:
//			values = data.length;
//			break;
//		case INT16:
//			values = data.length / Shorts.BYTES;
//			break;
//		case INT32:
//			values = data.length / Ints.BYTES;
//			break;
//		default:
//			throw new IllegalArgumentException("Not BytesAlignedStore " + codeType);
//		}
		return newStore(codeType, data, 0, -1);
	}


	public static ZIntStore newStore(Codec codeType, byte[] data, int offset,
			int numOfVal) {
		ZIntStore store;
		switch (codeType) {
		case INT8:
			store = new ZInt8Store(data, offset, numOfVal);
			break;
		case INT16:
			store = new ZInt16Store(data, offset, numOfVal);
			break;
		case INT32:
			store = new ZInt32Store(data, offset, numOfVal);
			break;
		default:
			throw new IllegalArgumentException("Not BytesAlignedStore " + codeType);
		}
		return store;
	}

	public static ZIntStore loadStore(ByteBuffer buffer, Codec codeType,
			int numOfValues) {

		ZIntStore store;
		switch (codeType) {
		case INT8:
			store = ZInt8Store.load(buffer, numOfValues);
			break;
		case INT16:
			store = ZInt16Store.load(buffer, numOfValues);
			break;
		case INT32:
			store = ZInt32Store.load(buffer, numOfValues);
			break;
		default:
			throw new IllegalArgumentException("Not BytesAlignedStore " + codeType);
		}
		return store;

	}

}
