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
public class AxisMeasure {

	private AggregatorType aggregator;
	
	private int field = -1;
	
	public AxisMeasure() {
		this(AggregatorType.UNKNOWN, 0);
	}


	public AxisMeasure(AggregatorType aggregator, int field) {
		this.aggregator = aggregator;
		this.field = field;
	}

	/**
	 * @return the aggregator
	 */
	public AggregatorType getAggregator() {
		return aggregator;
	}

	/**
	 * @param aggregator the aggregator to set
	 */
	public void setAggregator(AggregatorType aggregator) {
		this.aggregator = aggregator;
	}

	/**
	 * @return the field
	 */
	public int getField() {
		return field;
	}

	/**
	 * @param field the field to set
	 */
	public void setField(int field) {
		this.field = field;
	}
	
	
}
