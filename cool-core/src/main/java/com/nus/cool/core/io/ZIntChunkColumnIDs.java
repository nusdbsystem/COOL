package com.nus.cool.core.io;


import com.nus.cool.core.io.storevector.ZIntStore;
import com.nus.cool.core.io.storevector.ZIntStores;
import com.nus.cool.core.schema.Codec;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * @brief bytes-aligned-chunk-column-ids
 *
 */
public class ZIntChunkColumnIDs implements ChunkColumnIDs {

	private final ZIntStore zIntStore;

	private ZIntChunkColumnIDs(ZIntStore store) {
		this.zIntStore = store;
	}

	public ZIntChunkColumnIDs(Codec codeType, byte[] compressed) {
		checkNotNull(compressed);
		this.zIntStore = ZIntStores.newStore(codeType, compressed);
	}

	public static ChunkColumnIDs load(MappedByteBuffer buffer, Codec codeType,
			int num) {
		ZIntStore store = ZIntStores.loadStore(buffer, codeType, num);
		return new ZIntChunkColumnIDs(store);
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
	public boolean hasNext() {
		return this.zIntStore.hasNext();
	}

	@Override
	public int next() {
		return this.zIntStore.next();
	}

	@Override
	public String toString() {
//		List<Integer> array = Lists.newArrayList();
//		while (hasNext())
//			array.add(next());
		return this.zIntStore.toString();
	}

}
