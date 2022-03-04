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

import com.google.common.base.MoreObjects;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author David
 *
 */
public class PlanPhaseID {

	private QueryID qId;

	private int phaseId;

	public PlanPhaseID(QueryID qId, int phaseId) {
		this.qId = checkNotNull(qId);
		this.phaseId = phaseId;
	}


	/**
	 * @return the qId
	 */
	public QueryID getQueryID() {
		return qId;
	}

	/**
	 * @param qId
	 *          the qId to set
	 */
	public void setQueryID(QueryID qId) {
		this.qId = qId;
	}

	/**
	 * @return the phaseId
	 */
	public int getPhaseID() {
		return phaseId;
	}

	/**
	 * @param phaseId
	 *          the phaseId to set
	 */
	public void setPhaseID(int phaseId) {
		this.phaseId = phaseId;
	}

	@Override
	public int hashCode() {
		return Objects.hash(qId, phaseId);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof PlanPhaseID))
			return false;
		PlanPhaseID that = (PlanPhaseID) obj;
		return Objects.equals(this.qId, that.qId)
				&& this.phaseId == that.getPhaseID();
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).addValue(qId)
				.add("PhaseID", phaseId).toString();
	}

}
