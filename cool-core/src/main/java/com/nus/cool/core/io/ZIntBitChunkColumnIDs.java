package com.nus.cool.core.io;


import com.nus.cool.core.io.storevector.ZIntBitInputVector;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;


public class ZIntBitChunkColumnIDs implements ChunkColumnIDs {
    private final ZIntBitInputVector bitPackingStore;

    //public BitPackingChunkColumnIDs (byte[] ids, int offset, int len, int num, int bitWidth) {
    //    bitPackingStore = new BitPackingStore(ids, offset, len, num, bitWidth);
    //}

    public ZIntBitChunkColumnIDs (byte[] ids, int offset, int len) {
        this.bitPackingStore = ZIntBitInputVector.load(ids, offset, len);
    }

    public ZIntBitChunkColumnIDs (byte[] ids) {
        this(ids, 0, ids.length);
    }

    private ZIntBitChunkColumnIDs (ZIntBitInputVector store) {
    	this.bitPackingStore = store;
    }

    public static ZIntBitChunkColumnIDs load(MappedByteBuffer buffer) {
        checkNotNull(buffer);
        ZIntBitInputVector store = ZIntBitInputVector.load(buffer);
        ZIntBitChunkColumnIDs ids = new ZIntBitChunkColumnIDs(store);
        return ids;
    }
    
    public void groupBy(int[] vals, int[] cnts) {
    	this.bitPackingStore.groupBy(vals, cnts);
    }

    @Override 
    public void readFrom(ByteBuffer buffer) {
    	throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        bitPackingStore.writeTo(out);
    }

    @Override
    public boolean hasNext() {
        return bitPackingStore.hasNext();
    }

    @Override
    public int next() {
        return (int) bitPackingStore.next();
    }
    
	@Override
	public String toString() {
		return this.bitPackingStore.toString();
//		List<Integer> array = Lists.newArrayList();
//        for (int i = 0; i < bitPackingStore.size(); i++) {
//            array.add((int)bitPackingStore.get(i));
//        }
//		return array.toString();
	}
}

