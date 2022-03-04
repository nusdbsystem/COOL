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

package com.nus.cool.core.olap.query;

import com.google.common.collect.Lists;
import com.nus.cool.core.io.Chunk;
import com.nus.cool.core.io.ChunkDimension;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;


public class ChunkAxisFilter implements AxisFilter {

	private int[] fields;


	private List<int[]> includeSet = Lists.newArrayList();

	private List<int[]> excludeSet = Lists.newArrayList();

	private boolean hasFilter;

	private void fillIn(Chunk chunk, List<int[]> cubletMembers,
						List<int[]> chunkMembers) {
		ChunkDimension[] dimensions = new ChunkDimension[fields.length];
		for (int i = 0; i < dimensions.length; i++)
			dimensions[i] = (ChunkDimension) chunk.getDimensionColumn(fields[i]);

		for (int[] cubletMember : cubletMembers) {
			int[] chunkMember = new int[fields.length];
			boolean bHit = true;
			for (int i = 0; i < fields.length; i++) {
				int chunkID = dimensions[i].getDict().getLocalID(cubletMember[i]);
				if (chunkID > 0) {
					chunkMember[i] = chunkID;
				} else {
					bHit = false;
					break;
				}
			}
			if (bHit)
				chunkMembers.add(chunkMember);
		}
	}

	public ChunkAxisFilter(Chunk chunk, int[] fields, List<int[]> inSet,
			List<int[]> notInSet) {
		this.fields = checkNotNull(fields);
		this.hasFilter = !inSet.isEmpty() || !notInSet.isEmpty();
		fillIn(chunk, inSet, includeSet);
		fillIn(chunk, notInSet, excludeSet);
	}

	@Override
	public boolean isHit() {
		return !hasFilter() || !includeSet.isEmpty() || !excludeSet.isEmpty();
	}

	@Override
	public int[] getFields() {
		return fields;
	}

	@Override
	public boolean hasFilter() {
		return hasFilter;
	}

	@Override
	public List<int[]> getIncludeSet() {
		return includeSet;
	}

	@Override
	public List<int[]> getExcludeSet() {
		return excludeSet;
	}

}
