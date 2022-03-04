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
public class PlanTaskID {


	private PlanPhaseID phaseId;

	private int taskId;

	public PlanTaskID(PlanPhaseID phaseId, int taskId) {
		this.phaseId = checkNotNull(phaseId);
		this.taskId = taskId;
	}

	/**
	 * @return the phaseId
	 */
	public PlanPhaseID getPhaseID() {
		return phaseId;
	}

	/**
	 * @param phaseId
	 *          the phaseId to set
	 */
	public void setPhaseID(PlanPhaseID phaseId) {
		this.phaseId = phaseId;
	}

	/**
	 * @return the taskId
	 */
	public int getTaskID() {
		return taskId;
	}

	/**
	 * @param taskId
	 *          the taskId to set
	 */
	public void setTaskID(int taskId) {
		this.taskId = taskId;
	}

	@Override
	public int hashCode() {
		return Objects.hash(phaseId, taskId);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof PlanTaskID))
			return false;
		PlanTaskID that = (PlanTaskID) obj;
		return Objects.equals(this.phaseId, that.phaseId)
				&& this.taskId == that.taskId;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).addValue(phaseId)
				.add("TaskID", taskId).toString();
	}

}
