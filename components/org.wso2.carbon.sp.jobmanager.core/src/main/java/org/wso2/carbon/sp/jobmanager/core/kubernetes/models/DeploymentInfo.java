package org.wso2.carbon.sp.jobmanager.core.kubernetes.models;

/**
 * Contains details of a Deployment, which is responsible for containing a child Siddhi app
 */
public class DeploymentInfo {
    private String deploymentName;
    private String childSiddhiAppName;
}
