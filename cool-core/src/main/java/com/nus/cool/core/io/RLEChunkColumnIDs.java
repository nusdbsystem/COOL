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

import com.nus.cool.core.io.storevector.ZIntStore;
import com.nus.cool.core.io.storevector.ZIntStores;
import com.nus.cool.core.lang.Integers;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;

public class RLEChunkColumnIDs implements ChunkColumnIDs {

    private int numOfVal;
    private int maxVal;
    private int maxCount;

    private ZIntStore valStore;
    private ZIntStore cntStore;

    private int cntPos;
    private int valPos;
    private int curVal;
    private int curCnt;

    private void readMetaData(ByteBuffer buffer) {
        this.numOfVal = buffer.getInt();
        this.maxVal = buffer.getInt();
        this.maxCount = buffer.getInt();
    }

    private void init() {
        this.cntPos = 0;
        this.valPos = 0;
        cntStore.rewind();
        valStore.rewind();
        this.curCnt = cntStore.next();
        this.curVal = valStore.next();
    }

    public RLEChunkColumnIDs(byte[] compressed) {
        ByteBuffer buffer = ByteBuffer.wrap(compressed);
        buffer.order(ByteOrder.nativeOrder());
        
        int offset = 12;

        readMetaData(buffer);
        
        this.valStore = ZIntStores.newStore(
                ZIntStores.getCodeType(maxVal), compressed, offset,
                numOfVal);
        
        this.cntStore = ZIntStores.newStore(
                ZIntStores.getCodeType(maxCount), compressed, offset + valStore.sizeInByte(),
                numOfVal);

        init();
    }

    private RLEChunkColumnIDs() {}

    public static ChunkColumnIDs load(MappedByteBuffer buffer) {
        RLEChunkColumnIDs ids = new RLEChunkColumnIDs();
        ids.readMetaData(buffer);

        ids.valStore = ZIntStores.loadStore(buffer, 
                ZIntStores.getCodeType(ids.maxVal), ids.numOfVal);

        ids.cntStore = ZIntStores.loadStore(buffer, 
                ZIntStores.getCodeType(ids.maxCount), ids.numOfVal);

        ids.init();

        return ids;
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(Integers.toNativeByteOrder(numOfVal));
        out.writeInt(Integers.toNativeByteOrder(maxVal));
        out.writeInt(Integers.toNativeByteOrder(maxCount));      

        this.valStore.writeTo(out);
        this.cntStore.writeTo(out);
    }

    @Override
    public boolean hasNext() {
        return (this.valPos < this.numOfVal - 1
                || (this.cntPos < this.curCnt));    	
    }

    @Override
    public int next() {
        if (this.cntPos >= this.curCnt) {
                valPos++;
                curCnt += cntStore.next();
                curVal = valStore.next();
        }
        cntPos++;
        return curVal;
    }

    public void skipTo(int newPos) {
        if (newPos < cntPos) {
        	System.out.printf("newPos = %d, curPos = %d\n", newPos, cntPos);
        	throw new UnsupportedOperationException();
        }            

        while(this.curCnt <= newPos) {
            curCnt += cntStore.next();
            curVal = valStore.next();
        }

        cntPos = newPos;
    }
}
