/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.carbon.siddhi.extensions.installer.core.internal;

import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.siddhi.extensions.installer.core.constants.ExtensionsInstallerConstants;
import org.wso2.carbon.siddhi.extensions.installer.core.config.mapping.ConfigMapper;
import org.wso2.carbon.siddhi.extensions.installer.core.exceptions.ExtensionsInstallerException;
import org.wso2.carbon.siddhi.extensions.installer.core.config.mapping.models.ExtensionConfig;
import org.wso2.carbon.siddhi.extensions.installer.core.execution.DependencyInstaller;
import org.wso2.carbon.siddhi.extensions.installer.core.execution.DependencyInstallerImpl;
import org.wso2.carbon.siddhi.extensions.installer.core.execution.DependencyRetriever;
import org.wso2.carbon.siddhi.extensions.installer.core.execution.DependencyRetrieverImpl;
import org.wso2.carbon.siddhi.extensions.installer.core.execution.SiddhiAppExtensionUsageDetector;
import org.wso2.carbon.siddhi.extensions.installer.core.execution.SiddhiAppExtensionUsageDetectorImpl;
import org.wso2.msf4j.Microservice;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.wso2.carbon.siddhi.extensions.installer.core.models.enums.ExtensionInstallationStatus.NOT_INSTALLED;
import static org.wso2.carbon.siddhi.extensions.installer.core.models.enums.ExtensionUnInstallationStatus.NOT_UNINSTALLED;
import static org.wso2.carbon.siddhi.extensions.installer.core.util.ResponseEntityCreator.ACTION_STATUS_KEY;
import static org.wso2.carbon.siddhi.extensions.installer.core.util.ResponseEntityCreator.ACTION_TYPE_KEY;

/**
 * Exposes Siddhi Extensions Installer as a micro-service.
 */
@Component(
    name = "siddhi-extensions-installer",
    service = Microservice.class,
    immediate = true
)
@Path("/siddhi-extensions")
public class SiddhiExtensionsInstallerMicroservice implements Microservice {

    private static final Logger LOGGER = LoggerFactory.getLogger(SiddhiExtensionsInstallerMicroservice.class);

    private Map<String, ExtensionConfig> extensionConfigs;

    public SiddhiExtensionsInstallerMicroservice() throws Exception {
        System.out.println("SiddhiExtensionsInstallerMicroservice Constructor"); // TODO remove
        // Prevents instantiation.
    }

    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllExtensionStatuses() {
        try {
            DependencyRetriever dependencyRetriever = new DependencyRetrieverImpl(extensionConfigs);
            return Response
                .status(Response.Status.OK)
                .entity(dependencyRetriever.getAllExtensionStatuses())
                .type(MediaType.APPLICATION_JSON)
                .build();
        } catch (ExtensionsInstallerException e) {
            LOGGER.error("Failed to get installation statuses of all the extensions.", e);
            return Response
                .status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(e.getMessage())
                .build();
        }
    }

    @GET
    @Path("/status/{extensionId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getExtensionStatus(@PathParam("extensionId") String extensionId) {
        try {
            DependencyRetriever dependencyRetriever = new DependencyRetrieverImpl(extensionConfigs);
            return Response
                .status(Response.Status.OK)
                .entity(dependencyRetriever.getExtensionStatusFor(extensionId))
                .type(MediaType.APPLICATION_JSON)
                .build();
        } catch (ExtensionsInstallerException e) {
            LOGGER.error(String.format("Failed to get installation status of extension: %s.", extensionId), e);
            return Response
                .status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(e.getMessage())
                .build();
        }
    }

    @GET
    @Path("/status/{extensionId}/dependencies")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDependencyStatuses(@PathParam("extensionId") String extensionId) {
        try {
            DependencyRetriever dependencyRetriever = new DependencyRetrieverImpl(extensionConfigs);
            return Response
                .status(Response.Status.OK)
                .entity(dependencyRetriever.getDependencyStatusesFor(extensionId))
                .type(MediaType.APPLICATION_JSON)
                .build();
        } catch (ExtensionsInstallerException e) {
            LOGGER.error(String.format(
                "Failed to get installation statuses of dependencies, of extension: %s.", extensionId), e);
            return Response
                .status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(e.getMessage())
                .build();
        }
    }

    @POST
    @Path("/{extensionId}/install")
    @Produces(MediaType.APPLICATION_JSON)
    public Response installDependencies(@PathParam("extensionId") String extensionId) {
        try {
            DependencyInstaller dependencyInstaller = new DependencyInstallerImpl(extensionConfigs);
            Map<String, Object> responseEntity = dependencyInstaller.installDependenciesFor(extensionId);
            Response.Status responseStatus = isActionFailure(responseEntity) ? Response.Status.INTERNAL_SERVER_ERROR :
                Response.Status.OK;
            return Response
                .status(responseStatus)
                .entity(responseEntity)
                .type(MediaType.APPLICATION_JSON)
                .build();
        } catch (ExtensionsInstallerException e) {
            LOGGER.error(String.format("Failed to install dependencies for extension: %s.", extensionId), e);
            return Response
                .status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(e.getMessage())
                .build();
        }
    }

    @GET
    @Path("/{extensionId}/dependency-sharing-extensions")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDependencySharingExtensions(@PathParam("extensionId") String extensionId) {
        try {
            DependencyRetriever dependencyRetriever = new DependencyRetrieverImpl(extensionConfigs);
            return Response
                .status(Response.Status.OK)
                .entity(dependencyRetriever.getDependencySharingExtensionsFor(extensionId))
                .type(MediaType.APPLICATION_JSON)
                .build();
        } catch (ExtensionsInstallerException e) {
            LOGGER.error(
                String.format("Failed to get dependency sharing extensions for extension: %s.", extensionId), e);
            return Response
                .status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(e.getMessage())
                .build();
        }
    }

    @POST
    @Path("/{extensionId}/uninstall")
    @Produces(MediaType.APPLICATION_JSON)
    public Response uninstallDependencies(@PathParam("extensionId") String extensionId) {
        try {
            DependencyInstaller dependencyInstaller = new DependencyInstallerImpl(extensionConfigs);
            Map<String, Object> responseEntity = dependencyInstaller.unInstallDependenciesFor(extensionId);
            Response.Status responseStatus = isActionFailure(responseEntity) ? Response.Status.INTERNAL_SERVER_ERROR :
                Response.Status.OK;
            return Response
                .status(responseStatus)
                .entity(responseEntity)
                .type(MediaType.APPLICATION_JSON)
                .build();
        } catch (ExtensionsInstallerException e) {
            LOGGER.error(String.format("Failed to un-install dependencies for extension: %s.", extensionId), e);
            return Response
                .status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(e.getMessage())
                .build();
        }
    }

    /**
     * Returns whether the given response entity denotes a failure in installation/un-installation.
     * This information is used to mark such a failure as an internal server error.
     *
     * @param responseEntity Response entity produced by the installation/un-installation.
     * @return Whether the response entity denotes a failure in installation/un-installation.
     */
    private boolean isActionFailure(Map<String, Object> responseEntity) {
        return responseEntity.containsKey(ACTION_TYPE_KEY) && responseEntity.containsKey(ACTION_STATUS_KEY) &&
            (NOT_INSTALLED == responseEntity.get(ACTION_STATUS_KEY) ||
                NOT_UNINSTALLED == responseEntity.get(ACTION_STATUS_KEY));
    }

    @POST
    @Path("/siddhi-app-usages")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getUsedExtensionStatuses(String siddhiAppStringBase64) {
        try {
            String siddhiAppString =
                new String(Base64.getDecoder().decode(siddhiAppStringBase64), StandardCharsets.UTF_8);
            SiddhiAppExtensionUsageDetector siddhiAppExtensionUsageDetector =
                new SiddhiAppExtensionUsageDetectorImpl(extensionConfigs);
            return Response
                .status(Response.Status.OK)
                .entity(siddhiAppExtensionUsageDetector.getUsedExtensionStatuses(siddhiAppString))
                .build();
        } catch (ExtensionsInstallerException e) {
            LOGGER.error("Failed to get installation statuses of extensions used in the Siddhi app.", e);
            return Response
                .status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(e.getMessage())
                .build();
        }
    }

    public void hackyTest(String siddhiApp) { // TODO this is just here for testing
        SiddhiAppExtensionUsageDetector siddhiAppExtensionUsageDetector =
            new SiddhiAppExtensionUsageDetectorImpl(extensionConfigs);
        try {
            Map<String, Map<String, Object>> statuses =
                siddhiAppExtensionUsageDetector.getUsedExtensionStatuses(siddhiApp);
            LOGGER.info("Hacky Method got status keyset size as: " + statuses.keySet().size());
        } catch (ExtensionsInstallerException e) {
            LOGGER.error("Hacky method met an error", e);
        }
    }

    public String sayHello() {
        return "Hello World";
    }

    /**
     * This is the activation method of SiddhiExtensionsInstallerMicroservice.
     * This will be called when its references are satisfied.
     *
     * @param bundleContext the bundle context instance of this bundle.
     * @throws Exception Error occurred while executing the activate method.
     */
    @Activate
    protected void start(BundleContext bundleContext) throws Exception {
        // TODO moved this to constructor
        System.out.println("SiddhiExtensionsInstallerMicroservice @activate hit");
        extensionConfigs = ConfigMapper.loadAllExtensionConfigs(ExtensionsInstallerConstants.CONFIG_FILE_LOCATION);
    }

    /**
     * This is the deactivation method of SiddhiExtensionsInstallerMicroservice.
     * This will be called when this component is being stopped or references are satisfied during runtime.
     *
     * @throws Exception Error occurred while executing the de-activate method.
     */
    @Deactivate
    protected void stop() throws Exception {

    }

}
