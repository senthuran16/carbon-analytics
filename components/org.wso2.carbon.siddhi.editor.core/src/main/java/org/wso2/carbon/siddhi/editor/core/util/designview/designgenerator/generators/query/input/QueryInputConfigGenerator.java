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

package org.wso2.carbon.siddhi.editor.core.util.designview.designgenerator.generators.query.input;

import org.wso2.carbon.siddhi.editor.core.util.designview.beans.configs.siddhielements.query.input.QueryInputConfig;
import org.wso2.carbon.siddhi.editor.core.util.designview.beans.configs.siddhielements.query.input.patternsequence.PatternSequenceConfig;
import org.wso2.carbon.siddhi.editor.core.util.designview.constants.query.QueryInputType;
import org.wso2.carbon.siddhi.editor.core.util.designview.designgenerator.generators.query.input.types.JoinConfigGenerator;
import org.wso2.carbon.siddhi.editor.core.util.designview.designgenerator.generators.query.input.types.PatternConfigGenerator;
import org.wso2.carbon.siddhi.editor.core.util.designview.designgenerator.generators.query.input.types.SequenceConfigGenerator;
import org.wso2.carbon.siddhi.editor.core.util.designview.designgenerator.generators.query.input.types.WindowFilterProjectionConfigGenerator;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

/**
 * Generates QueryInputConfig object out of given Siddhi elements
 */
public class QueryInputConfigGenerator {
    private String siddhiAppString;
    private SiddhiApp siddhiApp;

    public QueryInputConfigGenerator(String siddhiAppString, SiddhiApp siddhiApp) {
        this.siddhiAppString = siddhiAppString;
        this.siddhiApp = siddhiApp;
    }

    /**
     * Generates Config for Query Input, from given Siddhi Query object and the complete Siddhi app string
     * @param queryInputStream      Siddhi Query InputStream object
     * @return                      QueryInputConfig object
     */
    public QueryInputConfig generateQueryInputConfig(InputStream queryInputStream) {
        // TODO: 4/25/18 THIS METHOD SHOULD BE REDONE
        String queryInputType = getQueryInputType(queryInputStream);

        if (queryInputType.equalsIgnoreCase(QueryInputType.WINDOW_FILTER_PROJECTION.toString())) {
            return new WindowFilterProjectionConfigGenerator(siddhiAppString)
                    .getWindowFilterProjectionQueryConfig(queryInputStream);
        } else if (queryInputType.equalsIgnoreCase(QueryInputType.JOIN.toString())) {
            return new JoinConfigGenerator().getJoinQueryConfig(queryInputStream, siddhiApp, siddhiAppString);
        } else if (queryInputType.equalsIgnoreCase(QueryInputType.PATTERN.toString())) {
            return new PatternConfigGenerator(siddhiAppString).getPatternQueryConfig(queryInputStream);
        } else if (queryInputType.equalsIgnoreCase(QueryInputType.SEQUENCE.toString())) {
            return new SequenceConfigGenerator(siddhiAppString).getSequenceQueryConfig(queryInputStream);
            // TODO: 4/9/18 implement for sequence
        }

//        if (queryInputType.equalsIgnoreCase(QueryInputType.WINDOW_FILTER_PROJECTION.toString())) {
//            return new WindowFilterProjectionConfigGenerator(siddhiAppString)
//                    .getWindowFilterProjectionQueryConfig(queryInputStream);
//        } else if (queryInputType.equalsIgnoreCase(QueryInputType.JOIN.toString())) {
//            return new JoinConfigGenerator().getJoinQueryConfig(queryInputStream, siddhiApp, siddhiAppString);
//        } else if (queryInputType.equalsIgnoreCase(QueryInputType.PATTERN.toString())) {
//            return new PatternConfigGenerator(siddhiAppString).getPatternQueryConfig(queryInputStream);
//        } else if (queryInputType.equalsIgnoreCase(QueryInputType.SEQUENCE.toString())) {
//            return new SequenceConfigGenerator(siddhiAppString).getSequenceQueryConfig(queryInputStream);
//            // TODO: 4/9/18 implement for sequence
//        }
        throw new IllegalArgumentException("Unknown type: " + queryInputType);
    }

    /**
     * Gets the type of the Query's Input, with the given Siddhi InputStream object
     * @param queryInputStream      Siddhi InputStream object, which contains data about the Query's input part
     * @return                      Type of Query's Input
     */
    private String getQueryInputType(InputStream queryInputStream) {
        if (queryInputStream instanceof SingleInputStream) {
            return QueryInputType.WINDOW_FILTER_PROJECTION.toString();
        } else if (queryInputStream instanceof JoinInputStream) {
            return QueryInputType.JOIN.toString();
        } else if (queryInputStream instanceof StateInputStream) {
            return ((StateInputStream) queryInputStream).getStateType().name(); // PATTERN or SEQUENCE
        }
        throw new IllegalArgumentException("Type of query is unknown for generating query input");
    }
}