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

/**
 * @author david
 *
 */
public class Axis {
	
	private AxisType axisType;
	
	private int[] fields = new int[0];
	
	private AxisMember[] includeMembers = new AxisMember[0];


	private AxisMember[] excludeMembers = new AxisMember[0];

	/**
	 * @return the axisType
	 */
	public AxisType getAxisType() {
		return axisType;
	}

	/**
	 * @param axisType the axisType to set
	 */
	public void setAxisType(AxisType axisType) {
		this.axisType = axisType;
	}

	/**
	 * @return the includesMember
	 */
	public AxisMember[] getIncludeMembers() {
		return includeMembers;
	}

	/**
	 * @param includeMembers the includesMember to set
	 */
	public void setIncludesMember(AxisMember... includeMembers) {
		this.includeMembers = includeMembers;
	}

	/**
	 * @return the excludesMember
	 */
	public AxisMember[] getExcludeMembers() {
		return excludeMembers;
	}

	/**
	 * @param excludesMember the excludesMember to set
	 */
	public void setExcludeMembers(AxisMember... excludeMembers) {
		this.excludeMembers = excludeMembers;
	}

	/**
	 * @return the fields
	 */
	public int[] getFields() {
		return fields;
	}

	/**
	 * @param fields the fields to set
	 */
	public void setFields(int... fields) {
		this.fields = fields;
	}
	
	public boolean hasFilter() {
		return includeMembers.length > 0 || excludeMembers.length > 0;
	}

}
