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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.swagger.annotations.ApiParam;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.service.component.annotations.Component;
import org.wso2.carbon.analytics.msf4j.interceptor.common.AuthenticationInterceptor;
import org.wso2.carbon.sp.jobmanager.core.factories.KubernetesManagerApiServiceFactory;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.DeploymentInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ManagerServiceInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.WorkerPodInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.deserializers.DeserializersRegisterer;
import org.wso2.msf4j.Microservice;
import org.wso2.msf4j.Request;
import org.wso2.msf4j.formparam.FormDataParam;
import org.wso2.msf4j.interceptor.annotation.RequestInterceptor;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

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

    @POST
    @Path("/worker-pods/metrics")
    @Consumes({"application/json"})
    @Produces({"application/json"})
    public Response getWorkerPodMetrics(@ApiParam(value = "Worker pods", required = true)
                                              List<WorkerPodInfo> workerPods) throws NotFoundException {
        return kubernetesManagerApi.getWorkerPodMetrics(workerPods);
    }

    @POST
    @Path("/siddhi-app")
    @Consumes({"application/x-www-form-urlencoded"})
    @Produces({"application/json"})
    public Response getChildSiddhiAppInfos(@FormDataParam(value = "userDefinedSiddhiApp") String userDefinedSiddhiApp,
                                           @FormDataParam(value = "kafkaIp") String kafkaIp,
                                           @FormDataParam(value = "kafkaPort") String kafkaPort)
            throws NotFoundException {
        return kubernetesManagerApi.getChildSiddhiAppInfos(userDefinedSiddhiApp, kafkaIp, kafkaPort);
    }

    @POST
    @Path("/worker-pods/deployments")
    @Consumes({"text/plain"})
    @Produces({"application/json"})
    public Response updateDeployments(@ApiParam(value = "Siddhi app deployments", required = true)
                                              String deployments) throws NotFoundException {
        Gson gson = DeserializersRegisterer.getGsonBuilder().disableHtmlEscaping().create();
        Type listType = new TypeToken<ArrayList<DeploymentInfo>>(){}.getType();
        List<DeploymentInfo> deploymentInfos = gson.fromJson(deployments, listType);
        return kubernetesManagerApi.updateDeployments(deploymentInfos);
    }

    @GET
    @Path("/is-active")
    public Response isActive (@Context Request request) {
        return Response.ok().entity("Kubernetes Manager is Active!").build();
    }
}
