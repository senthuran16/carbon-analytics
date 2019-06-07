package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components;

import feign.FeignException;
import feign.Response;
import org.apache.log4j.Logger;
import org.wso2.carbon.sp.jobmanager.core.api.ResourceServiceFactory;
import org.wso2.carbon.sp.jobmanager.core.impl.utils.Constants;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.DeploymentInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ResourceRequirement;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic.KubernetesChildAppDeployer;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components.resourcerequirementdetectors.SiddhiResourceRequirementDetectorsHolder;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.models.ChildSiddhiAppInfo;
import org.wso2.carbon.sp.jobmanager.core.util.HTTPSClientUtil;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.util.List;

/**
 * This class gives the details of the deployed siddhi application's details.
 */
public class KubernetesSiddhiAppDeployer implements KubernetesChildAppDeployer {
    private static final Logger LOG = Logger.getLogger(KubernetesSiddhiAppDeployer.class);

    @Override
    public boolean deploy(DeploymentInfo deployment) {
        Response resourceResponse = null;
        System.out.println("Attempting Deployment for: " +
                deployment.getChildAppInfo().getName() + " on: " + deployment.getWorkerPodInfo().getIp());
        try {
            resourceResponse = ResourceServiceFactory
                    .getResourceHttpsClient(
                            Constants.PROTOCOL + HTTPSClientUtil.generateURLHostPort(
                                    deployment.getWorkerPodInfo().getIp(), "9443"),
                            "admin", "admin") // TODO remove hardcoded
                    .postSiddhiApp(deployment.getChildAppInfo().getContent());
            System.out.println("Resource Response:" + resourceResponse);
            if (resourceResponse != null) {
                if (resourceResponse.status() == 200 || resourceResponse.status() == 201 ||
                        resourceResponse.status() == 409) { // Allow already existing Siddhi app conflict
                    return true;
                }
            }
            return false;
        } catch (FeignException e) {
            System.out.println("Failed to create deployment: " + deployment);
            return false;
        } finally {
            if (resourceResponse != null) {
                System.out.println("Closing Resource Response");
                resourceResponse.close();
            }
        }
    }
}
