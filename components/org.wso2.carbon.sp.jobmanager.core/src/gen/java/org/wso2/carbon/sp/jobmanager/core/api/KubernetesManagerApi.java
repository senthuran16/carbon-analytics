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

import io.swagger.annotations.ApiParam;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.service.component.annotations.Component;
import org.wso2.carbon.analytics.msf4j.interceptor.common.AuthenticationInterceptor;
import org.wso2.carbon.sp.jobmanager.core.factories.KubernetesManagerApiServiceFactory;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.ChildSiddhiAppInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.WorkerPodInfo;
import org.wso2.msf4j.Microservice;
import org.wso2.msf4j.Request;
import org.wso2.msf4j.interceptor.annotation.RequestInterceptor;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * Auto generated class from Swagger to MSF4J. // TODO class comment
 */

@Component(
        service = Microservice.class,
        immediate = true
)

@Path("/kubernetes-manager")

@RequestInterceptor(AuthenticationInterceptor.class)
@io.swagger.annotations.Api(description = "the managers API")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaMSF4JServerCodegen",
        date = "2018-01-29T08:19:07.148Z")
public class KubernetesManagerApi implements Microservice {
    private static final Log logger = LogFactory.getLog(KubernetesManagerApi.class);
    private final KubernetesManagerApiService kubernetesManagerApi =
            KubernetesManagerApiServiceFactory.getKubernetesManagerApi();

    @GET
    @Path("/worker-pods/metrics")
    @Produces({"application/json"}) // TODO define well
    @io.swagger.annotations.ApiOperation(value = "Get metrics of available worker pods in the Kubernetes cluster ",
            notes = "" + ".", response = void.class, tags = {"Managers",})
    @io.swagger.annotations.ApiResponses(value = {
            @io.swagger.annotations.ApiResponse(code = 200, message = "OK.", response = void.class),
            @io.swagger.annotations.ApiResponse(code = 404, message = "Current node is not the active node",
                    response = void.class),
            @io.swagger.annotations.ApiResponse(code = 500, message = "An unexpected error occured.",
                    response = void.class)})
    public Response getWorkerPodMetrics(@Context Request request)
            throws NotFoundException {
        return kubernetesManagerApi.getWorkerPodMetrics(request);
    }

    @POST
    @Path("/worker-pods/deployments")
    @Consumes({"application/json"})
    public Response updateDeployments(@ApiParam(value = "Worker pods and relevant Siddhi apps", required = true)
                                     Map<WorkerPodInfo, ChildSiddhiAppInfo> deployments) throws NotFoundException {
        return null; // TODO implement
    }
}
