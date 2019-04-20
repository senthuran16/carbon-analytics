package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic.resourcerequirementdetectors;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ResourceRequirement;

import java.util.Map;

public interface ResourceRequirementDetector<T,R> {
    ResourceRequirement generateResourceRequirement(T parsedApp, R appRuntime, String appString);

    Map<String, String> generateAffinityLabels();
}
