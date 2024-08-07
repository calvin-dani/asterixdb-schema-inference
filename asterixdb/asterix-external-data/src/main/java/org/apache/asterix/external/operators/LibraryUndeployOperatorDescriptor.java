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

package org.apache.asterix.external.operators;

import java.io.IOException;

import org.apache.asterix.common.metadata.Namespace;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class LibraryUndeployOperatorDescriptor extends AbstractLibraryOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger(LibraryUndeployOperatorDescriptor.class);

    public LibraryUndeployOperatorDescriptor(IOperatorDescriptorRegistry spec, Namespace namespace,
            String libraryName) {
        super(spec, namespace, libraryName);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractLibraryNodePushable(ctx) {
            @Override
            protected void execute() throws IOException {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Undeploying library {}.{}", namespace, libraryName);
                }

                try {
                    // #. close the library (close its open files if any)
                    closeLibrary();
                } finally {
                    // #. drop the whole library dir
                    FileReference libDir = getLibraryDir();
                    dropIfExists(libDir);
                    flushDirectory(libDir.getFile().getParentFile());
                }
            }
        };
    }
}
