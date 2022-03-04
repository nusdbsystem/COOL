/**
 * 
 */
package com.nus.cool.core.olap.query;

/**
 * @author david
 *
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
