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
import com.google.common.eventbus.Subscribe;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Phase of a query plan TODO: this class should be thread-safe
 * 
 * @author David
 * 
 */
public class ParallelPlanPhase implements PlanPhase {

	private PlanPhaseID phaseID;

	private List<PlanTask> tasks;

	private HashMap<PlanTaskID, PlanTask> taskMap = new HashMap<>();

	private AtomicInteger completedTasks;

	private AtomicInteger failedTasks;

	private int taskNums;

	private Vector<CellSet> resultVector = new Vector<>();

	private CellSet result;

	private EventBus eventBus;

	private AggregateType type;

	private Invoker invoker;

	public ParallelPlanPhase(PlanPhaseID phaseID, List<PlanTask> tasks,
			EventBus eventBus, AggregateType type) {
		this.phaseID = checkNotNull(phaseID);
		this.tasks = checkNotNull(tasks);
		this.eventBus = checkNotNull(eventBus);
		this.eventBus.register(this);
		this.type = type;
		this.taskNums = tasks.size();

		completedTasks.set(0);
		failedTasks.set(0);
	}

	@Override
	public boolean isParallel() {
		return true;
	}

	@Override
	public List<PlanTask> getPlanTasks() {
		return this.tasks;
	}

	@Override
	public PlanPhaseID getPlanPhaseID() {
		return phaseID;
	}

	@Override
	public void execute(Invoker invoker) throws Exception {
		setInvoker(invoker);
		for (PlanTask task : tasks) {
			PlanTaskID taskID = task.getTaskID();
			taskMap.put(taskID, task);
			invoker.invoke(task.getEndpoint(), task.getTask(),
					new PlanTaskResult(taskID, eventBus));
		}
	}

	@Subscribe
	public void onPlanTaskCompleted(PlanTaskCompletionEvent e) {
		if (e.isComplete()) {
			completedTasks.incrementAndGet();
			synchronized (resultVector) {
				resultVector.add(e.getResult());
			}
			System.err.println(e.getTaskID() + " is completed");

			if (completedTasks.get() == taskNums) {
				mergeResults();
			}
		} else {

			try {
				//TODO change the query server
				failedTasks.incrementAndGet();
				PlanTaskID taskID = e.getTaskID();
				PlanTask task = taskMap.get(taskID);
				this.invoker.invoke(task.getEndpoint(), task.getTask(),
						new PlanTaskResult(taskID, eventBus));
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	public int completedTasks() {
		return completedTasks.get();
	}

	public int failedTasks() {
		return failedTasks.get();
	}

	// TODO merge results from different tasks based on the phase type
	private void mergeResults() {
		try {
			switch (this.type) {
			case DISTRIBUTIVE:

				setResult(null);
				break;
			case ALGEBRAIC:

				setResult(null);
				break;
			case HOLISTIC:
				break;
			}
		} catch (Exception e) {
			throw e;
		}
	}

	/**
	 * Completes the phase and release all resources
	 */
	public void complete() {
		eventBus.unregister(this);
		phaseID = null;
		tasks = null;
		eventBus = null;
	}

	/**
	 * @return the result
	 */
	public CellSet getResult() {
		return result;
	}

	/**
	 * @param result
	 *            the result to set
	 */
	public void setResult(CellSet result) {
		this.result = result;
	}

	/**
	 * @return the invoker
	 */
	public Invoker getInvoker() {
		return invoker;
	}

	/**
	 * @param invoker
	 *            the invoker to set
	 */
	public void setInvoker(Invoker invoker) {
		this.invoker = invoker;
	}
}
