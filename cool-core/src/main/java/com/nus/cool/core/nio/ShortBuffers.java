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

import java.nio.ShortBuffer;


public class ShortBuffers {

	public static int binarySearch(ShortBuffer data, short key) {
		return binarySearch(data, data.position(), data.limit(), key);
	}
    
    public static int binarySearch(ShortBuffer data, int fromIndex, int toIndex,
			short key) {
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

	public static int binarySearchUnsigned(ShortBuffer data, short key) {
		return binarySearchUnsigned(data, data.position(), data.limit(), key);
	}

	public static int binarySearchUnsigned(ShortBuffer data, int fromIndex, int toIndex,
			short key) {
		--toIndex;
		
		// need to compare between the unsigned version of key and pivot
		// as both of them can be negative
		int ikey = key & 0xFFFF;
		while (fromIndex <= toIndex) {
			int mid = (fromIndex + toIndex) >> 1;
			int e = data.get(mid) & 0xFFFF;
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
