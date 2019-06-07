package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic.resourcerequirementdetectors;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ResourceRequirement;

import java.util.List;

/**
 * Keeps track of resource requirement detectors
 */
public interface ResourceRequirementDetectorsHolder {
    /**
     * Initiate all the known resource requirement detectors
     */
    void init();

    /**
     * Detects resource requirements for the given app, with all the known resource requirement detectors
     * @param appString
     * @return
     */
    List<ResourceRequirement> detectResourceRequirements(String appString);
}
