package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.DeploymentInfo;

public interface KubernetesChildAppDeployer {
    boolean deploy(DeploymentInfo deploymentInfo);
}
