/*
 * Copyright 2020 Cool Squad Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nus.cool.core.io;

import com.google.common.primitives.Ints;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkArgument;


public class LZ4CubletColumnTerms implements CubletColumnTerms {
	
	private static Log LOG = LogFactory.getLog(LZ4CubletColumnTerms.class);
	
	private byte[] data;

	private int[] offsets;

	private LZ4Compressor compressor;

	private LZ4FastDecompressor decompressor;

	private LZ4CubletColumnTerms() { 
		this.compressor = LZ4Factory.fastestInstance().fastCompressor();
		this.decompressor = LZ4Factory.fastestInstance().fastDecompressor();
	}
	
	public LZ4CubletColumnTerms(byte[] data, int[] offsets,
                                LZ4Compressor compressor, LZ4FastDecompressor decompressor) {
		this.data = data;
		this.offsets = offsets;
		this.compressor = compressor;
		this.decompressor = decompressor;
	}
	
	public static CubletColumnTerms load(MappedByteBuffer buffer) {
		CubletColumnTerms ccValues = new LZ4CubletColumnTerms();
		ccValues.readFrom(buffer);
		return ccValues;
	}

	@Override
	public void readFrom(ByteBuffer buffer) {
		int compressedLen = buffer.getInt();
		int rawLen = buffer.getInt();
		byte[] compressed = new byte[compressedLen];
		byte[] raw = new byte[rawLen];
		buffer.get(compressed);
		decompressor.decompress(compressed, raw, rawLen);
		ByteBuffer buf = ByteBuffer.wrap(raw).order(ByteOrder.nativeOrder());
		int values = buf.getInt();
		this.offsets = new int[values];
		for (int i = 0; i < values; i++)
			this.offsets[i] = buf.getInt();
		this.data = new byte[rawLen - 4 - values * 4];
		buf.get(this.data);
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		int rawLength = 4 + offsets.length * Ints.BYTES + data.length;
		ByteBuffer buffer = ByteBuffer.allocate(rawLength).order(ByteOrder.nativeOrder());
		buffer.putInt(offsets.length);
		for (int off : offsets)
			buffer.putInt(off);
		buffer.put(data);
		byte[] buf = buffer.array();
		int maxCompressedLength = compressor.maxCompressedLength(rawLength);
		byte[] compressed = new byte[maxCompressedLength];
		int compressedLen = compressor.compress(buf, 0, rawLength, compressed, 0,
				maxCompressedLength);
		
		boolean bLittle = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
		out.writeInt(bLittle ? Integer.reverseBytes(compressedLen) : compressedLen);
		out.writeInt(bLittle ? Integer.reverseBytes(rawLength) : rawLength);
		out.write(compressed, 0, compressedLen);
		LOG.info("LZ4 terms stat " + String.format("(rawLen = %d, zLen = %d)", rawLength, 8 + compressedLen));
	}

	@Override
	public String getTerm(int globalId, Charset charset) {
		checkArgument(globalId < offsets.length && globalId >= 0);
		int last = offsets.length - 1;
		int off = offsets[globalId];
		int end = globalId == last ? data.length : offsets[globalId + 1];
		int len = end - off;
		byte[] tmp = new byte[len];
		System.arraycopy(data, off, tmp, 0, len);
		return new String(tmp, charset);
	}

	@Override
	public int terms() {
		return offsets.length;
	}
}
