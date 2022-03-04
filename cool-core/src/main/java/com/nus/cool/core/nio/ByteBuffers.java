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
package com.nus.cool.core.nio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ByteBuffers {

	public static ByteBuffer wrapAsNativeByteOrder(byte[] array) {
		return ByteBuffer.wrap(array, 0, array.length).order(ByteOrder.nativeOrder());
	}

	public static ByteBuffer wrapAsNativeByteOrder(byte[] array, int offset, int length) {
		return ByteBuffer.wrap(array, offset, length).order(ByteOrder.nativeOrder());
	}
	
	public static int binarySearch(ByteBuffer data, byte key) {
		return binarySearch(data, data.position(), data.limit(), key);
	}
    
    public static int binarySearch(ByteBuffer data, int fromIndex, int toIndex,
			byte key) {
		--toIndex;
		
		int ikey = key;
		while (fromIndex <= toIndex) {
			int mid = (fromIndex + toIndex) >> 1;
			int e = data.get(mid);
			if (ikey > e)
				fromIndex = mid + 1;
			else if (ikey < e)
				toIndex = mid - 1;
			else
				return mid; // key found
		}
		return ~fromIndex;
	}


	public static int binarySearchUnsigned(ByteBuffer data, byte key) {
		return binarySearchUnsigned(data, data.position(), data.limit(), key);
	}
	
	public static int binarySearchUnsigned(ByteBuffer data, int fromIndex, int toIndex,
			byte key) {
		checkNotNull(data);
		checkArgument(fromIndex < data.limit() && toIndex <= data.limit());
		
		// need to compare between the unsigned version of key and pivot
		// as both of them can be negative
		int ikey = key & 0xFF;
		--toIndex;
		while (fromIndex <= toIndex) {
			int mid = (fromIndex + toIndex) >> 1;
			int e = data.get(mid) & 0xFF;
			if (ikey > e)
				fromIndex = mid + 1;
			else if (ikey < e)
				toIndex = mid - 1;
			else
				return mid; // key found
		}
		return ~fromIndex;
	}
}
