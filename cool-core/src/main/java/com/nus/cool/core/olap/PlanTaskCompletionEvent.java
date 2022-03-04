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

import static com.google.common.base.Preconditions.checkNotNull;
/**
 * Represent a completed PlanTask
 * 
 * @author david
 *
 */
public class PlanTaskCompletionEvent {
	
	private PlanTaskID taskID;
	
	private CellSet result;
	
	private boolean complete = true;
	
	public PlanTaskCompletionEvent(PlanTaskID taskID, CellSet result) {
		this(taskID, true, result);
	}
	
	public PlanTaskCompletionEvent(PlanTaskID taskID, boolean complete) {
		this(taskID, false , null);
	}
	
	public PlanTaskCompletionEvent(PlanTaskID taskID, boolean complete, CellSet result) {
		this.taskID = checkNotNull(taskID);
		setComplete(complete);
		setResult(result);
	}


	/**
	 * @return the taskID
	 */
	public PlanTaskID getTaskID() {
		return taskID;
	}

	/**
	 * @param taskID the taskID to set
	 */
	public void setTaskID(PlanTaskID taskID) {
		this.taskID = taskID;
	}

	/**
	 * @return the complete
	 */
	public boolean isComplete() {
		return complete;
	}

	/**
	 * @param complete the complete to set
	 */
	public void setComplete(boolean complete) {
		this.complete = complete;
	}

	/**
	 * @return the result
	 */
	public CellSet getResult() {
		return result;
	}

	/**
	 * @param result the result to set
	 */
	public void setResult(CellSet result) {
		this.result = result;
	}
	
	

}
