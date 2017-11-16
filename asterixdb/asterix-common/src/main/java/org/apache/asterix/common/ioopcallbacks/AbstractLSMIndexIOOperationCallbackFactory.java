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

package org.apache.asterix.common.ioopcallbacks;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGeneratorFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;

public abstract class AbstractLSMIndexIOOperationCallbackFactory implements ILSMIOOperationCallbackFactory {

    private static final long serialVersionUID = 1L;

    protected final ILSMComponentIdGeneratorFactory idGeneratorFactory;

    protected transient INCServiceContext ncCtx;

    public AbstractLSMIndexIOOperationCallbackFactory(ILSMComponentIdGeneratorFactory idGeneratorFactory) {
        this.idGeneratorFactory = idGeneratorFactory;
    }

    @Override
    public void initialize(INCServiceContext ncCtx) {
        this.ncCtx = ncCtx;
    }

    protected ILSMComponentIdGenerator getComponentIdGenerator() {
        assert ncCtx != null;
        return idGeneratorFactory.getComponentIdGenerator(ncCtx);
    }
}