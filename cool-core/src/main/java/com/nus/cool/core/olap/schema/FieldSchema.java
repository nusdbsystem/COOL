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

import sg.edu.nus.comp.aeolus.core.olap.schema.TableSchema.FieldDef;

/**
 * @author david
 *
 */
public class FieldSchema {
	
	private int fieldID;
	
	private FieldDef fieldDef;
	
	private boolean isKeyField;
	
	private TableSchema tableSchema;
	
	public FieldSchema(TableSchema tableSchema, int fieldID, FieldDef fieldDef) {
		this.tableSchema = tableSchema;
		this.fieldID = fieldID;
		this.fieldDef = fieldDef;
		int[] key = tableSchema.getKey();
		for(int i = 0; i < key.length; i++)
			if(key[i] == fieldID) {
				this.isKeyField = true;
				break;
			}
	}
	
	public FieldType getFieldType() {
		return fieldDef.getFieldType();
	}
	
	public DataType getDataType() {
		return fieldDef.getDataType();
	}
	
	public int getScale() {
		return fieldDef.getScale();
	}
	
	public boolean isKeyField() {
		return this.isKeyField;
	}
	
	public TableSchema getTableSchema() {
		return this.tableSchema;
	}
	
	public int getFieldID() {
		return this.fieldID;
	}

}
