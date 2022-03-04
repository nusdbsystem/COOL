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
 */
public class Query {
	
	private Axis[] axes = new Axis[0];

	private AxisMeasure[] measures = new AxisMeasure[0];
	
	private int slicer = -1;

	/**
	 * @return the axes
	 */
	public Axis[] getAxes() {
		return axes;
	}


	/**
	 * @param axes the axes to set
	 */
	public void setAxes(Axis... axes) {
		this.axes = axes;
		for(int i = 0; i < axes.length; i++) {
			if(axes[i].getAxisType() == AxisType.SLICER) {
				slicer = i;
				break;
			}
		}
	}

	/**
	 * @return the measures
	 */
	public AxisMeasure[] getMeasures() {
		return measures;
	}

	/**
	 * @param measures the measures to set
	 */
	public void setMeasures(AxisMeasure... measures) {
		this.measures = measures;
	}
	
	public boolean hasSlicer() {
		return slicer != -1;
	}
	
	public Axis getSlicer() {
		return axes[slicer];
	}
	
}
