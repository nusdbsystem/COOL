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
package com.nus.cool.core.cohort;

import com.nus.cool.core.cohort.aggregator.EventAggregator;
import com.nus.cool.core.cohort.filter.AgeFieldFilter;
import com.nus.cool.core.cohort.filter.FieldFilter;
import com.nus.cool.core.cohort.filter.FieldFilterFactory;
import com.nus.cool.core.cohort.filter.SetFieldFilter;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.DataType;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ExtendedCohortSelection implements Operator {

	private final FieldFilterFactory filterFactory = new FieldFilterFactory();

	private final List<Map<String, FieldFilter>> birthFilters = new ArrayList<>();

	private final List<Map<String, FieldFilter>> birthAggFilters = new ArrayList<>();

	// Record the minimal time frequencies of birth events
	private int[] minTriggerTime;

	// Record the maximal time frequencies of birth events
	private int[] maxTriggerTime;

	private final Map<String, FieldFilter> ageFilters = new HashMap<>();

	private final Map<String, FieldFilter> ageByFilters = new HashMap<>();

	private TableSchema tableSchema;

	private InputVector timeVector;

	private int maxDate;

	private boolean bUserActiveCublet;

	private boolean bUserActiveChunk;

	private boolean bAgeActiveChunk;

	private FieldFilter ageFilter;

	private ExtendedCohortQuery q;

	private final ArrayList<LinkedList<Integer>> eventOffset = new ArrayList<>();

	private final ExtendedCohort cohort = new ExtendedCohort();

	private ChunkRS chunk;


	public void init(TableSchema tableSchema, ExtendedCohortQuery q) {
		this.tableSchema = checkNotNull(tableSchema);
		this.q = checkNotNull(q);   

		// process birth selector
		BirthSequence seq = q.getBirthSequence();
		this.minTriggerTime = new int[seq.getBirthEvents().size()];
		this.maxTriggerTime = new int[seq.getBirthEvents().size()];

		int idx = 0;
		for (BirthSequence.BirthEvent e : seq.getBirthEvents()) {
			Map<String, FieldFilter> filters = new HashMap<>();
			Map<String, FieldFilter> aggrFilters = new HashMap<>();

			// handle birth selection filters
			for (ExtendedFieldSet fs : e.getEventSelection()) {
				String fn = fs.getCubeField();
				FieldSchema schema = tableSchema.getField(fn);
//				checkArgument(schema.getDataType() != OutputCompressor.DataType.Aggregate);
				filters.put(fn, FieldFilterFactory.create(schema, fs, fs.getFieldValue().getValues()));
			}

			// handle aggregate selection filters
			for (ExtendedFieldSet fs : e.getAggrSelection()) {
				String fn = fs.getCubeField();
				FieldSchema schema = tableSchema.getField(fn);
				aggrFilters.put(fn, FieldFilterFactory.create(schema, fs, fs.getFieldValue().getValues()));
			}

			this.birthFilters.add(filters);
			this.birthAggFilters.add(aggrFilters);

			// this.birthFilterFields.add(field);
			minTriggerTime[idx] = e.getMinTrigger();
			maxTriggerTime[idx] = e.getMaxTrigger();

			this.eventOffset.add(new LinkedList<Integer>());
			++idx;
		}

		// Process age-by and age selectors
		if (q.getAgeField() == null) return;

		for (int i = 0; i < 2; i++) {
			List<ExtendedFieldSet> selectors;
			Map<String, FieldFilter> filterMap;
			if (i == 0) {
				int fieldID = tableSchema.getFieldID(q.getAgeField().getField());       
//				checkArgument(fieldID >= 0 &&
//						tableSchema.getField(fieldID).getDataType() != DataType.Aggregate);
				selectors = q.getAgeField().getEventSelection();
				List<String> ageRange = q.getAgeField().getRange();
				if (ageRange == null || ageRange.isEmpty()) {
					//hardcode an AgeFieldFilter
					ageRange = new ArrayList<>();
					ageRange.add(String.valueOf(Integer.MIN_VALUE)
							+ "|" + String.valueOf(Integer.MAX_VALUE));
				}
				this.ageFilter = new AgeFieldFilter(ageRange);

				//                if (fieldID == tableSchema.getActionTimeField())
				//                	continue;
				filterMap = this.ageByFilters;
			} else {
				selectors = q.getAgeSelection();
				filterMap = this.ageFilters;
			}

			if (selectors == null) continue;
			for (ExtendedFieldSet fs : selectors) {
				String field = fs.getCubeField();                                       
				filterMap.put(field,
						FieldFilterFactory.create(tableSchema.getField(field), fs, fs.getFieldValue().getValues()));
			}
		}
	}

	@Override
	public void init(TableSchema schema, CohortQuery query) {

	}

	@Override
	public void process(MetaChunkRS metaChunk) {
		bUserActiveCublet = true;

		// Process birth filter
		int idx = 0;
		for (Map<String, FieldFilter> birthFilter : birthFilters) {
			for (Map.Entry<String, FieldFilter> entry : birthFilter.entrySet()) {
				MetaFieldRS metaField = metaChunk.getMetaField(entry.getKey());
				if (minTriggerTime[idx] > 0)
					bUserActiveCublet &= entry.getValue().accept(checkNotNull(metaField));
				else
					entry.getValue().accept(metaField);
			}
			idx++;
		}

		// process birth aggregation filter
		for (Map<String, FieldFilter> birthFilter : this.birthAggFilters) {
			for (Map.Entry<String, FieldFilter> entry : birthFilter.entrySet()) {
				bUserActiveCublet &= entry.getValue().accept(metaChunk.getMetaField(entry.getKey()));
			}
		}

		// Process age by and age filter
		for (Map.Entry<String, FieldFilter> entry : ageFilters.entrySet()) {
			MetaFieldRS metaField = metaChunk.getMetaField(entry.getKey());
			entry.getValue().accept(checkNotNull(metaField));
		}

		// age by event/dimension
		for (Map.Entry<String, FieldFilter> entry : ageByFilters.entrySet()) {
			MetaFieldRS metaField = metaChunk.getMetaField(entry.getKey());
			entry.getValue().accept(checkNotNull(metaField));
		}

		this.maxDate = TimeUtils.getDate(
				metaChunk.getMetaField(tableSchema.getActionTimeField(), FieldType.ActionTime).getMaxValue());
	}

	@Override
	public void process(ChunkRS chunk) {

		bUserActiveChunk = true;
		bAgeActiveChunk = true;

		this.chunk = chunk;

		for (int i = 0; i < birthFilters.size(); i++) {
			Map<String, FieldFilter> filterMap = birthFilters.get(i);

			for (Map.Entry<String, FieldFilter> entry : filterMap.entrySet()) {
				FieldRS field = chunk.getField(entry.getKey());
				if (minTriggerTime[i] > 0)
					bUserActiveChunk &= entry.getValue().accept(field);
				else
					entry.getValue().accept(field);
			}
		}

		// process birth aggregation filter
		for (Map<String, FieldFilter> birthFilter : this.birthAggFilters) {
			for (Map.Entry<String, FieldFilter> entry : birthFilter.entrySet()) {
				bUserActiveChunk &= entry.getValue().accept(chunk.getField(entry.getKey()));
			}
		}

		for (Map.Entry<String, FieldFilter> entry : ageFilters.entrySet()) {
			FieldRS field = chunk.getField(entry.getKey());
			bAgeActiveChunk &= entry.getValue().accept(field);
			// ageFilterFields.put(entry.getKey(), field);
		}

		for (Map.Entry<String, FieldFilter> entry : ageByFilters.entrySet()) {
			FieldRS field = chunk.getField(entry.getKey());
			bAgeActiveChunk &= entry.getValue().accept(field);
			// ageByFilterFields.put(entry.getKey(), field);
		}

		timeVector = chunk.getField(tableSchema.getActionTimeField()).getValueVector();
	}

	/**
	 * @brief find the next tuple where the given @param event is qualified with
	 *        respect to the birth filter
	 *
	 * @param event
	 * @param fromOffset
	 * @param endOffset
	 *
	 * @return the offset of the qualified tuple, or @param endOffset if not
	 *         found
	 */
	private int skipToNextQualifiedBirthTuple(int event, int fromOffset, int endOffset) {

		int passed = 0;
		int nextOffset;
		int numFilter = birthFilters.get(event).entrySet().size();
		if (fromOffset >= endOffset || numFilter == 0)
			return fromOffset;
		while (true) {
			for (Map.Entry<String, FieldFilter> entry : birthFilters.get(event).entrySet()) {
				nextOffset = entry.getValue().nextAcceptTuple(fromOffset, endOffset);
				if (nextOffset > fromOffset) {
					fromOffset = nextOffset;
					passed = 1;
				} else {
					passed++;
				}
				if (passed == numFilter || fromOffset >= endOffset)
					return fromOffset;
			}
		}
	}

	/**
	 * @brief check if the number of occurrence of each birth event follows the
	 *        max/min trigger condition or not
	 *
	 * @return true if follows; false otherwise
	 */
	private boolean checkOccurrence(int e) {
		int occurrence = this.eventOffset.get(e).size();
		if (minTriggerTime[e] > occurrence || (maxTriggerTime[e] >= 0 && maxTriggerTime[e] < occurrence))
			return false;
		return true;
	}

	/**
	 * @brief check for each birth event if its aggregated value passes the
	 *        aggregation filter or not
	 *
	 * @return true if passes; false otherwise
	 */
	private boolean filterByBirthAggregation() {
		boolean r = true;
		FieldFilter filter;
		Map<String, FieldFilter> filters;
		for (int i = 0; r && i < birthAggFilters.size(); i++) {
			filters = this.birthAggFilters.get(i);

			for (Map.Entry<String, FieldFilter> entry : filters.entrySet()) {
				Double birthAttr = getBirthAttribute(i, tableSchema.getFieldID(entry.getKey()));

				if (birthAttr == null)
					return false; // aggregation value does not exist

				if (entry.getValue() instanceof SetFieldFilter) {
					r = r && entry.getValue().accept(birthAttr.intValue());
				} else {
					filter = entry.getValue();
					ExtendedFieldSet.FieldValue value = filter.getFieldSet().getFieldValue();
					if (value.getType() != ExtendedFieldSet.FieldValueType.AbsoluteValue) {
						filter.updateValues(
								getBirthAttribute(value.getBaseEvent(), tableSchema.getFieldID(value.getBaseField())));
					}
					r = r && filter.accept(birthAttr.intValue());
				}

				if (!r)
					return r;
			}
		}
		return r;
	}

	/**
	 * @brief evaluate the birth filters against tuples within the given offset
	 *        range
	 *
	 * @param eventId
	 *            the id of birth event
	 * @param fromOffset
	 *            the starting point
	 * @param endOffset
	 */
	private void filterEvent(int eventId, int fromOffset, int endOffset) {
		LinkedList<Integer> offset = eventOffset.get(eventId);
		while (fromOffset < endOffset) {
			fromOffset = skipToNextQualifiedBirthTuple(eventId, fromOffset, endOffset);
			if (fromOffset < endOffset) {
				offset.addLast(fromOffset);
				fromOffset++;
			}
		}
	}

	private int getUserBirthTime(int start, int end) {
		BirthSequence seq = q.getBirthSequence();
		List<Integer> sortedEvents = seq.getSortedBirthEvents();
		List<Integer> windowtDate = new ArrayList<Integer>(sortedEvents.size());
		int offset = start;
		int birthOffset = start;
		int firstDay = TimeUtils.getDate(timeVector.get(start));
		LinkedList<Integer> occurrences;
		int birthDay = firstDay;

		// starting date of each event time window
		for (int i = 0; i < sortedEvents.size(); i++) {
			windowtDate.add(0);
		}

		for (Integer e : sortedEvents) {
			offset = start;
			BirthSequence.BirthEvent event = seq.getBirthEvents().get(e);

			for (Map.Entry<String, FieldFilter> entry : birthFilters.get(e).entrySet()) {
				FieldFilter ageFilter = entry.getValue();
				ExtendedFieldSet.FieldValue value = entry.getValue().getFieldSet().getFieldValue();
				if (value.getType() != ExtendedFieldSet.FieldValueType.AbsoluteValue) {
					ageFilter.updateValues(
							getBirthAttribute(value.getBaseEvent(), tableSchema.getFieldID(value.getBaseField())));
				}
			}

			// check time window
			BirthSequence.TimeWindow window = event.getTimeWindow();
			if (window == null) {
				// no time window
				for (int i = 0; i < minTriggerTime[e]; i++) {
					offset = skipToNextQualifiedBirthTuple(e, offset, end);
					if (offset == end)
						return -1;
					eventOffset.get(e).addLast(offset);
					offset++;
				}
				int bday;
				if (minTriggerTime[e] == 0)
					bday = TimeUtils.getDate(timeVector.get(offset));
				else
					bday = TimeUtils.getDate(timeVector.get(offset - 1)) + 1;

				birthDay = (birthDay < bday) ? bday : birthDay;
			} else {
				// with time window
				int startDay = firstDay;
				int wlen = window.getLength();
				int endDay = maxDate - (wlen - 1); // at least one time window
				int windowOffset;

				for (BirthSequence.Anchor anc : window.getAnchors()) {
					int day = anc.getLowOffset() + windowtDate.get(anc.getAnchor());
					startDay = startDay < day ? day : startDay;
					day = anc.getHighOffset() + windowtDate.get(anc.getAnchor());
					endDay = endDay > day ? day : endDay;
				}

				if (wlen == 0) {
					wlen = maxDate - startDay + 1;
					endDay = (endDay >= startDay) ? startDay : endDay;
				}

				offset = TimeUtils.skipToDate(timeVector, start, end, startDay);
				windowOffset = offset;
				while (startDay <= endDay) {
					// skip to the endOffset for the time window
					int pos = TimeUtils.skipToDate(timeVector, offset, end, startDay + wlen);

					// delete offset beyond the time window
					occurrences = eventOffset.get(e);
					windowOffset = TimeUtils.skipToDate(timeVector, windowOffset, end, startDay);
					while (!occurrences.isEmpty()) {
						if (occurrences.getFirst() >= windowOffset)
							break;
						occurrences.removeFirst();
					}

					// evaluate birth filters between the time range
					filterEvent(e, offset, pos);
					offset = pos;

					if (checkOccurrence(e)) {
						windowtDate.set(e, startDay);
						birthDay = (birthDay < startDay + wlen) ? startDay + wlen : birthDay;
						break;
					}

					// not a slice window and occurrence check fails
					if (!window.getSlice()) {
						return -1;
					}

					startDay++;
				}

				if (startDay > endDay)
					return -1;
			}

			// birthOffset points to the first age tuple
			birthOffset = (birthOffset < offset) ? offset : birthOffset;
		}

		for (Integer e : sortedEvents) {
			// filter between [start, birthOffset] for birth events
			// without time window
			if (seq.getBirthEvents().get(e).getTimeWindow() == null) {
				offset = (eventOffset.get(e).isEmpty()) ? start : eventOffset.get(e).getLast() + 1;
				filterEvent(e, offset, birthOffset);
				if (!checkOccurrence(e))
					return -1;
			}
		}

		// evaluate birth aggregation filters
		if (filterByBirthAggregation()) {
			// real birthday
			cohort.setBirthDate(birthDay - 1);
			cohort.setBirthOffset(birthOffset);
			return birthOffset;
		}

		else
			return -1;
	}

	public ExtendedCohort selectUser(int start, int end) {
		checkArgument(start < end);

		// clean event offset
		for (LinkedList<Integer> occurrence : this.eventOffset) {
			occurrence.clear();
		}

		int boff = this.getUserBirthTime(start, end);

		cohort.clearDimension();

		if (boff >= 0) {
			// find the respective cohort for this user
			List<BirthSequence.BirthEvent> events = q.getBirthSequence().getBirthEvents();
			for (int idx = 0; idx < events.size(); ++idx) {
				BirthSequence.BirthEvent be = events.get(idx);
				for (BirthSequence.CohortField cf : be.getCohortFields()) {

					int fieldID = tableSchema.getFieldID(cf.getField());                    

					// cohort by the birth time
					if (fieldID == tableSchema.getActionTimeField()) {
						cohort.addDimension(TimeUtils.getDateofNextTimeUnitN(cohort.getBirthDate(),
								q.getAgeField().getUnit(), 0));
						continue;
					}

					Double value = getBirthAttribute(idx, fieldID);
					if (value == null)
						return null;

					// determine the dimension
					FieldSchema schema = tableSchema.getField(fieldID);
					if (schema.getDataType() == DataType.String) {
						// use the global id as the cohort dimension
						int gid = chunk.getField(cf.getField()).getKeyVector().get(value.intValue());
						cohort.addDimension(gid);
					} else {
						int level = cf.getMinLevel();
						Double v;
						if (cf.isLogScale()) {
							while (true) {
								v = Math.pow(cf.getScale(), level);
								if (value <= v || level >= cf.getNumLevel() + cf.getMinLevel()) {
									break;
								}
								++level;
							}
						} else {
							v = (value / cf.getScale());
							level = (v > v.intValue()) ? v.intValue()+1 : v.intValue();
							level = (level >= cf.getMinLevel()) ? level : cf.getMinLevel();
							if (level >= cf.getMinLevel() + cf.getNumLevel())
								level = cf.getMinLevel() + cf.getNumLevel();
						}
						cohort.addDimension(level);
					}
				}
			}

			return cohort;
		}

		return null;
	}
	
	private void updateStats(int age, int val, Map<Integer, List<Double>> cohortCells) {
		if (cohortCells != null) {
			if (cohortCells.get(age) == null) {
				cohortCells.put(age, new ArrayList<Double>());
				cohortCells.get(age).add(0.0);
			}
			if (cohortCells.get(age).size() < 5) {
				cohortCells.get(age).add(Double.MAX_VALUE);
				cohortCells.get(age).add(-1.0 * Double.MAX_VALUE);
				cohortCells.get(age).add(0.0);
				cohortCells.get(age).add(0.0);
			}
			if (val < cohortCells.get(age).get(1))
				cohortCells.get(age).set(1, (double)val);
			if (val > cohortCells.get(age).get(2))
				cohortCells.get(age).set(2, (double)val);
			cohortCells.get(age).set(3, cohortCells.get(age).get(3) + val);
			cohortCells.get(age).set(4, cohortCells.get(age).get(4) + 1);
		}
	}

	private void filterAgeActivity(int ageOff, int ageEnd, BitSet bs, InputVector fieldIn, 
			FieldFilter ageFilter, Map<Integer, List<Double>> cohortCells, boolean updateStats) {
		// update value for this column if necessary
//		ExtendedFieldSet.FieldValue value = ageFilter.getFieldSet().getFieldValue();
//		if (value.getType() != FieldValueType.AbsoluteValue) {
//			ageFilter.updateValues(
//					getBirthAttribute(value.getBaseEvent(), tableSchema.getFieldID(value.getBaseField())));
//		}

		fieldIn.skipTo(ageOff);
		if (bs.cardinality() >= ((ageEnd - ageOff) >> 1)) {
			for (int i = ageOff; i < ageEnd; i++) {
				int val = fieldIn.next();
				if (!ageFilter.accept(val)) {
					bs.clear(i);
					// deleteStats(i - ageOff, cohortCells);
				}
				else {
					if (updateStats)
						updateStats(i - ageOff, val, cohortCells);
				}
			}
		} else {            
			int pos = bs.nextSetBit(ageOff);
			while (pos < ageEnd && pos >= 0) {
				int val = fieldIn.get(pos);
				if (!ageFilter.accept(val)) {
					bs.clear(pos);
					// deleteStats(pos - ageOff, cohortCells);
				}
				else {
					if (updateStats)
						updateStats(pos - ageOff, val, cohortCells);
				}
				pos = bs.nextSetBit(pos + 1);
			}
		}        
	}

	public void selectAgeByActivities(int ageOff, int ageEnd, BitSet bs) {
		checkArgument(ageOff < ageEnd);

		for (Map.Entry<String, FieldFilter> entry : ageByFilters.entrySet()) {
			this.filterAgeActivity(ageOff, ageEnd, bs, chunk.getField(entry.getKey()).getValueVector(),
					entry.getValue(), null, false);
		}

		// age by dimension
		// each qualified activity will make the positions of all neighbouring 
		// activities with the same dimension value set
		int fieldID = tableSchema.getFieldID(q.getAgeField().getField());
		if (fieldID != tableSchema.getActionField() &&
				fieldID != tableSchema.getActionTimeField()) {   
			InputVector inputVector = this.chunk.getField(fieldID).getValueVector();
			int pos = ageOff;
			inputVector.skipTo(pos);
			int lastVal = inputVector.next();
			while (pos < ageEnd) {                
				int oldPos = pos;
				while (++pos < ageEnd) {
					int v = inputVector.next();
					if (v != lastVal) {
						lastVal = v;
						break;
					}
				}
				int setBit = bs.nextSetBit(oldPos);
				if (setBit >= 0 && setBit < pos) {
					bs.set(oldPos, pos);
				}
			}
		}
	}

	/**
	 * Select age activity tuples bounded by [ageOff, ageEnd)
	 * 
	 * @param ageOff
	 *            the start position of age tuples
	 * @param ageEnd
	 *            the end position of age tuples
	 * @param bs
	 *            the hit position list of all qualified tuples
	 */
	public void selectAgeActivities(int ageOff, int ageEnd, BitSet bs, 
			BitSet ageDelimiters, Map<Integer, List<Double>> cohortCells) {
		checkArgument(ageOff < ageEnd);
		// enable the dimension-based ageby operator to be processed in the same way
		// as event-based ageby operator
		int fieldID = tableSchema.getFieldID(q.getAgeField().getField());
		if (fieldID != tableSchema.getActionField() &&
				fieldID != tableSchema.getActionTimeField()) {        	
			bs.and(ageDelimiters);
			int pos = ageDelimiters.nextSetBit(ageOff);
			int lastSetbit = pos;
			InputVector inputVector = this.chunk.getField(fieldID).getValueVector();
			while(pos >= 0 && pos < ageEnd) {
				lastSetbit = pos;
				inputVector.skipTo(pos);
				int lastValue = inputVector.next();                
				while(++pos < ageEnd) {
					if (ageDelimiters.get(pos) && inputVector.next() == lastValue) {
						ageDelimiters.clear(pos);
						lastSetbit = pos;
					}
					else {
						pos = ageDelimiters.nextSetBit(pos);                        
						break;
					}
				}
			}

			// clear the first setbit and set the the bit next to the last set bit
			// so as to enable the resulting delimiters 
			// to be consistent with event-based ageby operator
			if ((pos = ageDelimiters.nextSetBit(ageOff)) >= 0) {
				ageDelimiters.clear(pos);
				checkArgument(!ageDelimiters.get(lastSetbit + 1));
				ageDelimiters.set(lastSetbit + 1);
			}
		}

		// Columnar processing strategy ...
		// TODO: now we update the stats (min, max, avg) only if the field is "value"
		for (Map.Entry<String, FieldFilter> entry : ageFilters.entrySet()) {
			FieldFilter ageFilter = entry.getValue();
			filterAgeActivity(ageOff, ageEnd, bs, chunk.getField(entry.getKey()).getValueVector(), 
					ageFilter, cohortCells, (entry.getKey().equals("value")));            
		}      
	}

	private Double getBirthAttribute(int baseEvent, int fieldID) {
		FieldSchema schema = tableSchema.getField(fieldID);
		EventAggregator aggregator;

		if (schema.getDataType() != DataType.Aggregate)
			aggregator = BirthAggregatorFactory.getAggregator("UNIQUE");
		else {
			aggregator = BirthAggregatorFactory.getAggregator(schema.getAggregator());
//			fieldID = tableSchema.getFieldID(schema.getBaseField());
		}
		aggregator.init(chunk.getField(fieldID).getValueVector());
		return aggregator.birthAggregate(eventOffset.get(baseEvent));
	}

	public boolean isUserActiveCublet() {
		return this.bUserActiveCublet;
	}

	public boolean isUserActiveChunk() {
		return this.bUserActiveChunk;
	}

	public boolean isAgeActiveChunk() {
		return this.bAgeActiveChunk;
	}

	public FieldFilter getAgeFieldFilter(String atFieldName) {
		return this.ageFilters.get(atFieldName);
	}

	public FieldFilter getAgeFieldFilter() {
		return this.ageFilter;
	}

	public Object getCubletResults() {
		return null;
	}

	public void close() throws IOException {
	}
}
