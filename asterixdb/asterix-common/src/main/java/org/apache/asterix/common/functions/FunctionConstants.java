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
package org.apache.asterix.common.functions;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public final class FunctionConstants {

    public static final String ASTERIX_NS = "asterix";
    public static final String ASTERIX_DB = "asterix";

    public static final DataverseName ASTERIX_DV = DataverseName.createBuiltinDataverseName(ASTERIX_NS);

    public static final DataverseName ALGEBRICKS_DV =
            DataverseName.createBuiltinDataverseName(AlgebricksBuiltinFunctions.ALGEBRICKS_NS);

    private FunctionConstants() {
    }

    //TODO(DB): should be in a better place than this
    public static FunctionIdentifier newAsterix(String name, int arity) {
        return new FunctionIdentifier(ASTERIX_DB, ASTERIX_NS, name, arity);
    }
}
