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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

import static com.google.common.base.Preconditions.checkArgument;

public class ZIntBitInputVector implements ZIntStore, InputVector {

  private final LongBuffer bitPack;

  private final int capacity;
  private final int bitWidth;
  private final int noValPerPack;
  private final long mask;

  private int pos;
  private long curPack;
  private int packOffset;

  /**
   *
   * @param buf
   *          a byte array containing the compressed data
   * @param offset
   *          offset within the buffer
   * @param len
   *          length of buffer for storage
   */
  private ZIntBitInputVector(byte[] buf, int offset, int len) {

    ByteBuffer buffer = ByteBuffer.wrap(buf, offset, len);
    buffer.order(ByteOrder.nativeOrder());
    this.capacity = buffer.getInt();
    this.bitWidth = buffer.getInt();
    this.noValPerPack = 64 / bitWidth;
    this.mask = (bitWidth == 64) ? -1 : (1L << bitWidth) - 1;

    int numOfBytes = getNumOfBytes(capacity, bitWidth);

    if (len < numOfBytes) {
      System.out.printf("len = %d, numOfBytes = %d\n", len, numOfBytes);
      throw new java.nio.BufferUnderflowException();
    }

    // the first 8 bytes of the buffer are for metadata
    // the following bytes are for compressed data, organized as a long buffer
    this.bitPack = ByteBuffer.wrap(buf, offset + 8, numOfBytes - 8)
            .order(ByteOrder.nativeOrder()).asLongBuffer();

    rewind();
  }


  private ZIntBitInputVector(LongBuffer buffer, int capacity, int bitWidth) {
    this.capacity = capacity;
    this.bitWidth = bitWidth;
    this.noValPerPack = 64 / bitWidth;
    this.mask = (bitWidth == 64) ? -1 : (1L << bitWidth) - 1;
    this.bitPack = buffer;
  }

  public static ZIntBitInputVector load(ByteBuffer buffer) {
    int capacity = buffer.getInt();
    int width = buffer.getInt();
    int size = getNumOfBytes(capacity, width);
    int oldLimit = buffer.limit();
    buffer.limit(buffer.position() + size - 8);
    LongBuffer tmpBuffer = buffer.asLongBuffer();
    buffer.position(buffer.position() + size - 8);
    buffer.limit(oldLimit);
    return new ZIntBitInputVector(tmpBuffer, capacity, width);
  }

  public static ZIntBitInputVector load(byte[] compress) {
    return load(compress, 0, compress.length);
  }

  public static ZIntBitInputVector load(byte[] compress, int offset, int len) {
    return new ZIntBitInputVector(compress, offset, len);
  }

  private static int getNumOfBytes(int num, int width) {
    int i = 64 / width;
    int size = (num - 1) / i + 2;
    return size << 3;
  }

  @Override
  public int size() {
    return this.capacity;
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
    throw new UnsupportedOperationException();
  }

  @Override
  public int get(int index) {
      if (index >= this.capacity) {
          throw new IndexOutOfBoundsException();
      }
    int offset = 64 - (1 + index % this.noValPerPack) * this.bitWidth;
    long pack = getPack(index);
    long val = ((pack >>> offset) & this.mask);
    return (int) val;
  }

  @Override
  public void put(int[] val, int offset, int length) {

  }

  @Override
  public void put(int index, int val) {

  }

  @Override
  public int binarySearch(int key) {
    int from = 0;
    int to = capacity - 1;
    int mid;
    int midVal;

    while (from <= to) {
      mid = (from + to) >> 1;
      midVal = get(mid);

      if (key > midVal)
        from = mid + 1;
      else {
        if (key < midVal)
          to = mid - 1;
        else
          return mid;
      }
    }

    return -1;
  }

  public void groupBy(int[] vals, int[] cnt) {
    checkArgument(vals.length == cnt.length);
    long[] extVals = new long[vals.length];
    long mask1 = 0, mask2 = 0;
    int diff = 64 - noValPerPack * bitWidth;
    int width = bitWidth - 1;
    int i, j;

    for (j = 0; j < this.noValPerPack; j++) {
      mask1 = (mask1 << this.bitWidth) + (1 << width) - 1;
      mask2 = (mask2 << this.bitWidth) + (1 << width);
    }

    mask1 <<= diff;
    mask2 <<= diff;

    for (i = 0; i < vals.length; i++) {
      extVals[i] = 0;
      for (j = 0; j < this.noValPerPack; j++) {
        extVals[i] = (extVals[i] << this.bitWidth) + vals[i];
      }
      extVals[i] <<= diff;
      cnt[i] = 0;
    }

    int pos = this.bitPack.position();
    bitPack.rewind();
    long cur;
    long tmp;
    while (bitPack.hasRemaining()) {
      cur = bitPack.get();
      for (i = 0; i < extVals.length; i++) {
        tmp = cur ^ extVals[i];
        tmp += mask1;
        tmp = (~tmp);
        tmp &= mask2;
        cnt[i] += Long.bitCount(tmp);
      }
    }
    this.bitPack.position(pos);
  }


  @Override
  public boolean hasNext() {
    return this.pos < this.capacity;
  }

  @Override
  public int next() {
    return (int) nextLong();
  }

  @Override
  public void skipTo(int pos) {
      if (pos >= this.capacity) {
          throw new IndexOutOfBoundsException();
      }
    this.pos = pos;
    this.packOffset = 64 - (pos % this.noValPerPack) * this.bitWidth;
    this.bitPack.position(pos / this.noValPerPack);
    this.curPack = this.bitPack.get();
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeTo(DataOutput out) throws IOException {

  }

  private long getPack(int pos) {
    int idx = pos / this.noValPerPack;
    return this.bitPack.get(idx);
  }

  private long nextLong() {
    if (this.packOffset < this.bitWidth) {
      this.curPack = this.bitPack.get();
      this.packOffset = 64;
    }
    this.pos++;
    this.packOffset -= this.bitWidth;
    return ((this.curPack >>> this.packOffset) & this.mask);
  }
}
