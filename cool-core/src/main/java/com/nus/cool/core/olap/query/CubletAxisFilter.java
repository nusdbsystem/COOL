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
import sg.edu.nus.comp.aeolus.core.io.CubletReadStore;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author david
 *
 */
public class CubletAxisFilter implements AxisFilter {
	
	private Axis axis;
	
	private boolean isHit;
	
	private List<int[]> includeSet = Lists.newArrayList();
	
	private List<int[]> excludeSet = Lists.newArrayList();
	
	private void fillIn(CubletReadStore rs, AxisMember[] memberSet, List<int[]> memberIDs) {
		int[] fields = axis.getFields();
		for(AxisMember member : memberSet) {
			String[] memberParts = member.getMember();
			int[] termID = new int[fields.length];
			boolean bHit = true;
			for(int i = 0; i < fields.length; i++) {
				int id = rs.getCubletFingers(fields[i]).getTermID(memberParts[i]);
				if(id > 0) {
					termID[i] = id;
				} else {
					bHit = false;
					break;
				}				
			}
			if(bHit)
				memberIDs.add(termID);
		}
	}
	
	public CubletAxisFilter(CubletReadStore rs, Axis axis) {
		this.axis = checkNotNull(axis);
		
		fillIn(rs, axis.getIncludeMembers(), includeSet);
		fillIn(rs, axis.getExcludeMembers(), excludeSet);
		
		this.isHit = !includeSet.isEmpty() || !excludeSet.isEmpty();
	}

	@Override
	public boolean isHit() {
		return !hasFilter() || isHit;
	}

	@Override
	public int[] getFields() {
		return axis.getFields();
	}

	@Override
	public boolean hasFilter() {
		return axis.hasFilter();
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
