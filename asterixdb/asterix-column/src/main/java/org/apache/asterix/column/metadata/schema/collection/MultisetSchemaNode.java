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
package org.apache.asterix.column.metadata.schema.collection;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.IObjectSchemaNodeVisitor;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public final class MultisetSchemaNode extends AbstractCollectionSchemaNode {
    public MultisetSchemaNode() {
        super();
    }

    public MultisetSchemaNode(DataInput input, Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels)
            throws IOException {
        super(input, definitionLevels);
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.MULTISET;
    }

    @Override
    public IValueReference getFieldName() {
        return null;
    }

    @Override
    public void setFieldName(IValueReference newFieldName) {

    }

    @Override
    public <R, T> R accept(IObjectSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return null;
    }

    @Override
    public AbstractSchemaNode getChild(int i) {
        return null;
    }

    @Override
    public int getNumberOfChildren() {
        return 0;
    }

    public ATypeTag getItemTypeTag() {
        return getItemNode().getTypeTag();
    }
}
