/*
 *  Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.wso2.carbon.sp.jobmanager.core.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.service.component.annotations.Component;
import org.wso2.carbon.analytics.permissions.bean.Permission;
import org.wso2.carbon.sp.jobmanager.core.api.KubernetesManagerApiService;
import org.wso2.carbon.sp.jobmanager.core.api.ManagersApiService;
import org.wso2.carbon.sp.jobmanager.core.api.NotFoundException;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.WorkerPodsMonitor;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.ChildSiddhiAppInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.WorkerPodInfo;
import org.wso2.msf4j.Request;

import javax.ws.rs.core.Response;
import java.util.Map;


/**
 * Distributed Siddhi Service Implementataion Class // TODO class comment
 */
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaMSF4JServerCodegen",
        date = "2018-01-29T08:19:07.148Z")
@Component(service = ManagersApiService.class, immediate = true)

public class KubernetesManagerApiServiceImpl extends KubernetesManagerApiService {
    private static final Log logger = LogFactory.getLog(KubernetesManagerApiServiceImpl.class);

    @Override
    public Response getWorkerPodMetrics(Request request) throws NotFoundException {
        WorkerPodsMonitor workerPodsMonitor = new WorkerPodsMonitor(); // TODO remove hardcoded
        WorkerPodInfo hardcodedPod = new WorkerPodInfo("test-app-group-1-1", "localhost", "test-app-group-1-1");
        return workerPodsMonitor.getMetrics(hardcodedPod);
    }

    @Override
    public Response updateDeployments(Map<WorkerPodInfo, ChildSiddhiAppInfo> deployments) throws NotFoundException {
        return null;
    }
}
