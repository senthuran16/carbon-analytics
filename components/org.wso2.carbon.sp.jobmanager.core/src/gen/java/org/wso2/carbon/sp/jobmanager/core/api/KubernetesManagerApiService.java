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

package org.wso2.carbon.sp.jobmanager.core.api;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.ChildSiddhiAppInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.DeploymentInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.WorkerPodInfo;
import org.wso2.msf4j.Request;

import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * Auto generated class from Swagger to MSF4J. // TODO class comment
 */

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaMSF4JServerCodegen",
        date = "2018-01-29T08:19:07.148Z")
public abstract class KubernetesManagerApiService {

    public abstract Response getWorkerPodMetrics(List<WorkerPodInfo> workerPods) throws NotFoundException;

    public abstract Response updateDeployments(List<DeploymentInfo> deployments) throws NotFoundException;

    public abstract Response getChildSiddhiAppInfos(String userDefinedSiddhiApp) throws NotFoundException;
}
