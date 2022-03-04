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

import com.google.common.eventbus.EventBus;

import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.core.Response;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * TODO: add a member to hold the result object
 * No need, I have put that in the eventbus(Jun)
 * 
 * @author David
 *
 */
public class PlanTaskResult implements InvocationCallback<Response> {
	
	private PlanTaskID taskID;
	
	private EventBus eventBus;
	
	public PlanTaskResult(PlanTaskID taskID, EventBus evenBus) {
		this.taskID = checkNotNull(taskID);
		this.eventBus = checkNotNull(eventBus);
	}

	@Override
	public void completed(Response response) {
		
		
		if(response.getStatus() == 200){
			eventBus.post(new PlanTaskCompletionEvent(taskID, true, response.readEntity(CellSet.class)));
		}

		else 
			eventBus.post(new PlanTaskCompletionEvent(taskID, false));
	}

	@Override
	public void failed(Throwable t) {
		eventBus.post(new PlanTaskCompletionEvent(taskID, false));
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

}
