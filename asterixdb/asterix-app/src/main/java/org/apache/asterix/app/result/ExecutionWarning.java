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
package org.apache.asterix.app.result;

import org.apache.asterix.common.api.ICodedMessage;
import org.apache.hyracks.api.exceptions.Warning;

public class ExecutionWarning implements ICodedMessage {

    private final int code;
    private final String message;

    public ExecutionWarning(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public static ICodedMessage of(Warning warning) {
        return new ExecutionWarning(1, warning.getMessage());
    }

    @Override
    public int getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public boolean isRetriable() {
        return false;
    }
}
