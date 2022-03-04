package com.nus.cool.core.io;

import com.google.common.base.MoreObjects;
import com.nus.cool.core.io.storevector.ZIntStore;
import com.nus.cool.core.io.storevector.ZIntStores;
import com.nus.cool.core.schema.Codec;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * @brief bytes-aligned-chunk-column-dict
 *
 */
public class ZIntChunkColumnDict implements ChunkColumnDict {

	private ZIntStore zIntStore;

	private ZIntChunkColumnDict(ZIntStore s) {
		this.zIntStore = s;
	}

	public ZIntChunkColumnDict(Codec codeType, byte[] compressed) {
		this.zIntStore = ZIntStores.newStore(codeType, compressed);
	}

	public static ChunkColumnDict load(MappedByteBuffer buffer, Codec codeType) {		
		int cnt = buffer.getInt();
		ZIntStore s = ZIntStores.loadStore(buffer, codeType, cnt);
		return new ZIntChunkColumnDict(s);
	}

	@Override
	public void readFrom(ByteBuffer buffer) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		zIntStore.writeTo(out);
	}

	@Override
	public int getGlobalID(int localId) {
		return this.zIntStore.get(localId);
	}

	@Override
	public int getLocalID(int globalId) {
		return zIntStore.binarySearch(globalId);
	}

	@Override
	public boolean contains(int... globalIDs) {
		for (int gID : globalIDs)
			if (getLocalID(gID) >= 0)
				return true;
		return false;
	}

	@Override
	public int getNumOfIDs() {
		return this.zIntStore.size();
	}

	@Override
	public int[] getGlobalIDs() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("size", this.zIntStore.size())
				.add("bytes", this.zIntStore.sizeInByte())
				.toString();
	}
}
