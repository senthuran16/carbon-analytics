package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic.resourcerequirementdetectors;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ResourceRequirement;

import java.util.List;

public interface ResourceRequirementDetectorsHolder {
    void init();

    List<ResourceRequirement> detectResourceRequirements(String appString);
}
