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

package org.apache.jackrabbit.oak.fixture;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;

public class DocumentMemoryFixture extends NodeStoreFixture {

    @Override
    public NodeStore createNodeStore() {
        DocumentMK.Builder builder = new DocumentMK.Builder();
        //do not reuse the whiteboard
        setWhiteboard(new DefaultWhiteboard());
        builder.setNoChildOrderCleanupFeature(Feature.newFeature("FT_NOCOCLEANUP_OAK-10660", getWhiteboard()));
        return builder.getNodeStore();
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        if (nodeStore instanceof DocumentNodeStore) {
            ((DocumentNodeStore) nodeStore).dispose();
        }
    }

    @Override
    public String toString() {
        return "DocumentNodeStore[Memory]";
    }
}