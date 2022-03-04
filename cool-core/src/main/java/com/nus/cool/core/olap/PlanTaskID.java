/**
 * 
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
