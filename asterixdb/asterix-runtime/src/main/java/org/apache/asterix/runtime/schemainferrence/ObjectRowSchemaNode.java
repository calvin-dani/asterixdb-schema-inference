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
package org.apache.asterix.runtime.schemainferrence;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.schemainferrence.lazy.IObjectRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.primitive.MissingRowFieldSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.util.annotations.CriticalPath;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntImmutableList;
import it.unimi.dsi.fastutil.ints.IntList;

public final class ObjectRowSchemaNode extends AbstractRowSchemaNestedNode {

    private IValueReference fieldName;

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.OBJECT;
    }

    public int getNumberOfChildren() {
        return children.size();
    }

    private final Int2IntMap fieldNameIndexToChildIndexMap;
    private final List<AbstractRowSchemaNode> children;

    public IValueReference getFieldName() {
        return fieldName;
    }

    @Override
    public void setFieldName(IValueReference newFieldName) {
        fieldName = newFieldName;
    }

    public ObjectRowSchemaNode(IValueReference fieldName) {
        fieldNameIndexToChildIndexMap = new Int2IntOpenHashMap();
        children = new ArrayList<>();
        //        this.fieldName = fieldName;
    }

    public ObjectRowSchemaNode() {
        fieldNameIndexToChildIndexMap = new Int2IntOpenHashMap();
        children = new ArrayList<>();
    }

    ObjectRowSchemaNode(DataInput input) throws IOException {

        int numberOfChildren = input.readInt();

        fieldNameIndexToChildIndexMap = new Int2IntOpenHashMap();
        deserializeFieldNameIndexToChildIndex(input, fieldNameIndexToChildIndexMap, numberOfChildren);

        children = new ArrayList<>();
        deserializeChildren(input, children, numberOfChildren);
    }

    public AbstractRowSchemaNode getOrCreateChild(IValueReference fieldName, ATypeTag childTypeTag,
            RowMetadata columnMetadata) throws HyracksDataException {
        int numberOfChildren = children.size();
        int fieldNameIndex = columnMetadata.getFieldNamesDictionary().getOrCreateFieldNameIndex(fieldName);
        int childIndex = fieldNameIndexToChildIndexMap.getOrDefault(fieldNameIndex, numberOfChildren);
        AbstractRowSchemaNode currentChild = childIndex == numberOfChildren ? null : children.get(childIndex);

        AbstractRowSchemaNode newChild = columnMetadata.getOrCreateChild(currentChild, childTypeTag);
        if (currentChild == null) {
            children.add(childIndex, newChild);
            fieldNameIndexToChildIndexMap.put(fieldNameIndex, childIndex);
        } else if (currentChild != newChild) {
            children.set(childIndex, newChild);
        }

        return newChild;
    }

    public void addChild(int fieldNameIndex, AbstractRowSchemaNode child) {
        int childIndex = children.size();
        fieldNameIndexToChildIndexMap.put(fieldNameIndex, childIndex);
        children.add(child);
    }

    public AbstractRowSchemaNode getChild(int fieldNameIndex) {
        if (fieldNameIndexToChildIndexMap.containsKey(fieldNameIndex)) {
            return children.get(fieldNameIndexToChildIndexMap.get(fieldNameIndex));
        }
        return MissingRowFieldSchemaNode.INSTANCE;
    }

    public void removeChild(int fieldNameIndex) {
        int childIndex = fieldNameIndexToChildIndexMap.remove(fieldNameIndex);
        children.remove(childIndex);
    }

    public List<AbstractRowSchemaNode> getChildren() {
        return children;
    }

    /**
     * Should not be used in a {@link CriticalPath}
     */
    public IntList getChildrenFieldNameIndexes() {
        return IntImmutableList.toList(fieldNameIndexToChildIndexMap.int2IntEntrySet().stream()
                .sorted(Comparator.comparingInt(Entry::getIntValue)).mapToInt(Entry::getIntKey));
    }

    //TODO Calvin Dani To utilize
    public boolean containsField(int fieldNameIndex) {
        return fieldNameIndexToChildIndexMap.containsKey(fieldNameIndex);
    }

    @Override
    public boolean isObjectOrCollection() {
        return true;
    }

    @Override
    public boolean isCollection() {
        return false;
    }

    @Override
    public <R, T> R accept(IRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    public void serialize(DataOutput output) throws IOException {
        output.write(ATypeTag.OBJECT.serialize());
        output.writeInt(children.size());
        for (Entry fieldNameIndexChildIndex : fieldNameIndexToChildIndexMap.int2IntEntrySet()) {
            output.writeInt(fieldNameIndexChildIndex.getIntKey());
            output.writeInt(fieldNameIndexChildIndex.getIntValue());
        }
        for (AbstractRowSchemaNode child : children) {
            child.serialize(output);
        }
    }

    public void abort(DataInputStream input) throws IOException {
        //        definitionLevels.put(this, new RunRowLengthIntArray());

        int numberOfChildren = input.readInt();

        fieldNameIndexToChildIndexMap.clear();
        deserializeFieldNameIndexToChildIndex(input, fieldNameIndexToChildIndexMap, numberOfChildren);

        children.clear();
        deserializeChildren(input, children, numberOfChildren);
    }

    private static void deserializeFieldNameIndexToChildIndex(DataInput input, Int2IntMap fieldNameIndexToChildIndexMap,
            int numberOfChildren) throws IOException {
        for (int i = 0; i < numberOfChildren; i++) {
            int fieldNameIndex = input.readInt();
            int childIndex = input.readInt();
            fieldNameIndexToChildIndexMap.put(fieldNameIndex, childIndex);
        }
    }

    private static void deserializeChildren(DataInput input, List<AbstractRowSchemaNode> children, int numberOfChildren)
            throws IOException {
        for (int i = 0; i < numberOfChildren; i++) {
            children.add(AbstractRowSchemaNode.deserialize(input));
        }
    }

    public final <R, T> R accept(IObjectRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }
}