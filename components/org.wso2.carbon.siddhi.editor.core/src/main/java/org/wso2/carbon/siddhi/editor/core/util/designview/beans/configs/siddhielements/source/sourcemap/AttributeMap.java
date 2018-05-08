/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.siddhi.editor.core.util.designview.beans.configs.siddhielements.source.sourcemap;

import org.wso2.carbon.siddhi.editor.core.util.designview.constants.SourceMapAttributeTypes;

import java.util.Map;

/**
 * Represents a map type 'attribute' element inside Map, of a Siddhi Source
 */ // TODO: 4/3/18 give the format properly
public class AttributeMap extends SourceMapAttribute {
    private Map<String, String> value;

    public AttributeMap(Map<String, String> value) {
        super(SourceMapAttributeTypes.MAP);
        this.value = value;
    }

    public Map<String, String> getValue() {
        return value;
    }
}