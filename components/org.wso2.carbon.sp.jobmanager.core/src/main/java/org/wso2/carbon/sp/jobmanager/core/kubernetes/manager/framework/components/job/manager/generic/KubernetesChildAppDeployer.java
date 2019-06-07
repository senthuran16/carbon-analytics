package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.DeploymentInfo;

/**
 * Deploys a child streaming application in a Worker, within the Kubernetes cluster
 */
public interface KubernetesChildAppDeployer {
    /**
     * Creates a deployment with the Worker and the child streaming application, that is contained in the deployment
     * info passed as the parameter
     * @param deploymentInfo
     * @return
     */
    boolean deploy(DeploymentInfo deploymentInfo);
}
