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

package org.wso2.carbon.siddhi.editor.core.util.designview.designgenerator.generators.query.input.types;

import org.wso2.carbon.siddhi.editor.core.util.designview.beans.configs.siddhielements.query.input.windowfilterprojection.QueryWindowConfig;
import org.wso2.carbon.siddhi.editor.core.util.designview.beans.configs.siddhielements.query.input.windowfilterprojection.WindowFilterProjectionQueryConfig;
import org.wso2.carbon.siddhi.editor.core.util.designview.utilities.ConfigBuildingUtilities;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.handler.Filter;
import org.wso2.siddhi.query.api.execution.query.input.handler.StreamHandler;
import org.wso2.siddhi.query.api.execution.query.input.handler.Window;
import org.wso2.siddhi.query.api.execution.query.input.stream.BasicSingleInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Generator to create a QueryInputConfig of type WindowFilterProjection
 */
public class WindowFilterProjectionConfigGenerator {
    private Query query;
    private String siddhiAppString;

    public WindowFilterProjectionConfigGenerator(Query query, String siddhiAppString) {
        this.query = query;
        this.siddhiAppString = siddhiAppString;
    }

    /**
     * Gets a WindowFilterProjectionQueryConfig object, from the given Siddhi Query object
     * @param query     Siddhi Query object
     * @return          WindowFilterProjectionQueryConfig object
     */
    public WindowFilterProjectionQueryConfig getWindowFilterProjectionQueryConfig(Query query) {
        switch (getType(query)) {
            case PROJECTION:
                return generateProjectionQueryInput();
            case FILTER:
                return generateFilterQueryInput();
            case WINDOW:
                return generateWindowQueryInput();
            default:
                throw new IllegalArgumentException("Unknown type: " + getType(query) +
                        " for generating Window-Filter-Projection Query Config");
        }
    }

    /**
     * Returns the type of WindowFilterProjection Config to generate, from the given Siddhi Query object
     * @param query     Siddhi Query object
     * @return          Type of WindowFilterProjection Query to generate
     */
    private WindowFilterProjectionQueryType getType(Query query) {
        List<StreamHandler> streamHandlers = ((SingleInputStream)(query.getInputStream())).getStreamHandlers();
        if (streamHandlers.isEmpty()) {
            return WindowFilterProjectionQueryType.PROJECTION;
        } else {
            for (StreamHandler streamHandler : streamHandlers) {
                if (streamHandler instanceof Window) {
                    return WindowFilterProjectionQueryType.WINDOW;
                }
            }
            return WindowFilterProjectionQueryType.FILTER;
        }
    }

    /**
     * Generates a QueryInputConfig of type Projection, with the given Siddhi Query
     * @return      WindowFilterProjectionQueryConfig object, with the configuration of a Projection query
     */
    private WindowFilterProjectionQueryConfig generateProjectionQueryInput() {
        return new WindowFilterProjectionQueryConfig(query.getInputStream().getUniqueStreamIds().get(0), "", null);
    }

    /**
     * Generates a QueryInputConfig of type Filter, with the given Siddhi Query
     * @return      WindowFilterProjectionQueryConfig object, with the configuration of a Filter query
     */
    private WindowFilterProjectionQueryConfig generateFilterQueryInput() {
        String from = query.getInputStream().getUniqueStreamIds().get(0);
        // Filter query will have just one StreamHandler, that's the Filter
        Filter filter = (Filter) ((BasicSingleInputStream) (query.getInputStream())).getStreamHandlers().get(0);
        String filterDefinition = ConfigBuildingUtilities.getDefinition(filter, siddhiAppString);
        return new WindowFilterProjectionQueryConfig(
                from, filterDefinition.substring(1, filterDefinition.length() - 1).trim(), null);
    }

    /**
     * Generates a QueryInputConfig of type Window, with the given Siddhi Query
     * @return      WindowFilterProjectionQueryConfig object, with the configuration of a Window query
     */
    private WindowFilterProjectionQueryConfig generateWindowQueryInput() {
        String mainFilter = null;
        String windowFilter = null;
        String function = null;
        List<String> parameters = new ArrayList<>();

        for (StreamHandler streamHandler : ((SingleInputStream)(query.getInputStream())).getStreamHandlers()) {
            if (streamHandler instanceof Filter) {
                String definition;
                // First Filter will be Query's, and the next one will be window's
                if (mainFilter == null) {
                    definition = ConfigBuildingUtilities.getDefinition(streamHandler, siddhiAppString);
                    mainFilter = definition.substring(1, definition.length() - 1).trim();
                } else {
                    definition = ConfigBuildingUtilities.getDefinition(streamHandler, siddhiAppString);
                    windowFilter = definition.substring(1, definition.length() - 1).trim();
                }
            } else if (streamHandler instanceof Window) {
                for (Expression expression : streamHandler.getParameters()) {
                    parameters.add(ConfigBuildingUtilities.getDefinition(expression, siddhiAppString));
                }
                function = ((Window)streamHandler).getName();
            }
        }

        return new WindowFilterProjectionQueryConfig(
                query.getInputStream().getUniqueStreamIds().get(0),
                mainFilter,
                new QueryWindowConfig(function, parameters, windowFilter));
    }

    /**
     * Specific Type of the WindowFilterProjection Query
     */
    private enum WindowFilterProjectionQueryType {
        PROJECTION,
        FILTER,
        WINDOW
    }
}