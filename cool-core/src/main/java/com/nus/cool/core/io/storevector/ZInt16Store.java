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

import com.google.common.primitives.Shorts;
import com.nus.cool.core.util.ShortBuffers;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;

public class ZInt16Store implements ZIntStore, InputVector {

  private int count;

  private ShortBuffer buffer;

  public ZInt16Store(int count) {
    this.count = count;
  }

  public ZInt16Store(byte[] compressed, int offset, int unused) {
    ByteBuffer b = ByteBuffer.wrap(compressed, offset, 4).order(ByteOrder.nativeOrder());
    count = b.getInt();
    int length = count * Shorts.BYTES;
    this.buffer = ByteBuffer.wrap(compressed, offset + 4, length)
            .order(ByteOrder.nativeOrder()).asShortBuffer();
  }



  public static ZIntStore load(ByteBuffer buffer, int n) {
    ZIntStore store = new ZInt16Store(n);
    store.readFrom(buffer);
    return store;
  }

  @Override
  public int size() {
    return this.count;
  }

  @Override
  public int sizeInByte() {
    return 0;
  }

  @Override
  public void rewind() {

  }

  @Override
  public int find(int key) {
      if (key > Short.MAX_VALUE || key < 0) {
          return -1;
      }
    return ShortBuffers.binarySearchUnsigned(this.buffer, 0, this.buffer.limit(), (short) key);
  }

  @Override
  public int get(int index) {
    return (this.buffer.get(index) & 0xFFFF);
  }

  @Override
  public void put(int[] val, int offset, int length) {

  }

  @Override
  public void put(int index, int val) {

  }

  @Override
  public int binarySearch(int key) {
    return 0;
  }

  @Override
  public boolean hasNext() {
    return this.buffer.hasRemaining();
  }

  @Override
  public int next() {
    return (this.buffer.get() & 0xFFFF);
  }

  @Override
  public void skipTo(int pos) {
    this.buffer.position(pos);
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    int limit = buffer.limit();
    int newLimit = buffer.position() + this.count * Shorts.BYTES;
    buffer.limit(newLimit);
    this.buffer = buffer.asShortBuffer();
    buffer.position(newLimit);
    buffer.limit(limit);
  }

  @Override
  public void writeTo(DataOutput out) throws IOException {

  }
}
