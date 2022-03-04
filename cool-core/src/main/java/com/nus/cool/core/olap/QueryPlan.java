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

package com.nus.cool.core.olap;

import com.google.common.collect.Lists;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;


public class QueryPlan {
	
	private QueryID qID;
	
	private List<PlanPhase> phases = Lists.newArrayList();


	public QueryPlan(QueryID qID) {
		this.qID = checkNotNull(qID);
	}
	
	public void addPlanPhase(PlanPhase phase) {
		phases.add(phase);
	}

	public List<PlanPhase> getPlanPhases() {
		return this.phases;
	}

	/**
	 * @return the qID
	 */
	public QueryID getQueryID() {
		return qID;
	}

	/**
	 * @param qID the qID to set
	 */
	public void setQueryID(QueryID qID) {
		this.qID = qID;
	}
		
}
