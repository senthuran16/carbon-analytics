package org.wso2.carbon.sp.jobmanager.core.kubernetes.models;

import java.util.Map;

/**
 * Contains information about a resource requirement of a child Siddhi app
 */
public class ResourceRequirement {
    Map<String, String> labels;

    public ResourceRequirement(Map<String, String> labels) {
        this.labels = labels;
    }
}
