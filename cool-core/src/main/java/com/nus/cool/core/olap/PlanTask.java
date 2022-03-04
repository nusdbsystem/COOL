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

import java.net.URI;

/**
 * TODO: will change the type of task from Object to
 * other specific one. 
 * 
 */
public class PlanTask {
	
	private PlanTaskID taskID;
	
	private Object task;
	
	private URI remote;

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
	 * @return the remote
	 */
	public URI getEndpoint() {
		return remote;
	}

	/**
	 * @param remote the remote to set
	 */
	public void setEndpoint(URI remote) {
		this.remote = remote;
	}

	/**
	 * @return the task
	 */
	public Object getTask() {
		return task;
	}

	/**
	 * @param task the task to set
	 */
	public void setTask(Object task) {
		this.task = task;
	}
	
	
	
}
