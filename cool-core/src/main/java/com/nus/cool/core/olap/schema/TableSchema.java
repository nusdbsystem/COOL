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

package com.nus.cool.core.olap.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The schema definition for physical cube table
 * 
 * @author david
 *
 */
public class TableSchema {
	
	public static class FieldDef {
		
		private FieldType fieldType;
		
		private DataType dataType;
		
		private int scale = 0;
		
		public FieldDef() { }
		
		public FieldDef(FieldType fieldType, DataType dataType) {
			this(fieldType, dataType, 0);
		}
		
		public FieldDef(FieldType fieldType, DataType dataType, int scale) {
			this.fieldType = fieldType;
			this.dataType = dataType;
			this.scale = scale;
		}

		/**
		 * @return the fieldType
		 */
		public FieldType getFieldType() {
			return fieldType;
		}

		/**
		 * @param fieldType the fieldType to set
		 */
		public void setFieldType(FieldType fieldType) {
			this.fieldType = fieldType;
		}

		/**
		 * @return the dataType
		 */
		public DataType getDataType() {
			return dataType;
		}

		/**
		 * @param dataType the dataType to set
		 */
		public void setDataType(DataType dataType) {
			this.dataType = dataType;
		}

		/**
		 * @return the scale
		 */
		public int getScale() {
			return scale;
		}

		/**
		 * @param scale the scale to set
		 */
		public void setScale(int scale) {
			this.scale = scale;
		}
		
	}
		
	private int[] key;
	
	private List<FieldDef> fields;
	
	private String charset;
	
	private FieldSchema[] fieldSchema = new FieldSchema[0];

	/**
	 * @return the key
	 */
	public int[] getKey() {
		return key;
	}

	/**
	 * @param key the key to set
	 */
	public void setKey(int... key) {
		this.key = key;
	}

	/**
	 * @return the fields
	 */
	public List<FieldDef> getFields() {
		return fields;
	}

	/**
	 * @param fields the fields to set
	 */
	public void setFields(List<FieldDef> fields) {
		this.fields = fields;
	}
		
	public FieldSchema getFieldSchema(int i) {
		checkArgument(i >= 0 && i < fields.size());
		if(fieldSchema.length == 0) {
			int fieldsSize = fields.size();
			this.fieldSchema = new FieldSchema[fieldsSize];
			for(int j = 0; j < fieldsSize; j++)
				fieldSchema[j] = new FieldSchema(this, j, fields.get(j));
		}
		return fieldSchema[i];
	}
	
	public int fields() {
		return fields.size();
	}
	
	@Override
	public String toString() {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	public TableSchema() { }
	
	public static TableSchema load(InputStream in) throws IOException {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		return mapper.readValue(in, TableSchema.class);
	}
	
	public static FieldDef newDimensionField(DataType dataType) {
		checkArgument(dataType != DataType.DECIMAL);
		return new FieldDef(FieldType.Dimension, dataType);
	}

	public static FieldDef newMeasureField(DataType dataType) {
		return newMeasureField(dataType, 0);
	}

	public static FieldDef newMeasureField(DataType dataType, int scale) {
		return new FieldDef(FieldType.Measure, dataType, scale);
	}

	/**
	 * @return the charset
	 */
	public String getCharset() {
		return charset;
	}

	/**
	 * @param charset the charset to set
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}

}
