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
package com.nus.cool.core.lang;

import java.nio.ByteOrder;


public class Longs {

	/**
	 * Find the minimum number of bits to represent the long
	 * @param l the long 
	 * @return the mimimum number of bits
	 */
	public static int minBits(long l) {
		return Long.SIZE - Long.numberOfLeadingZeros(l);
	}
	
	/**
	 * Find the minimum number of bytes to represent the long
	 * @param l the long
	 * @return the minimum number of bites
	 */
	public static int minBytes(long l) {
		int bits = minBits(l);
		if (bits == 0)
			return 1;
		return ((bits - 1) >>> 3) + 1;
	}
	
    /**
    *
    * @param v the long integer
    * @return the native order
    */
   public static long toNativeByteOrder(long v) {
       boolean bLittle = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
       return (bLittle ? Long.reverseBytes(v) : v);
   }
	
}
