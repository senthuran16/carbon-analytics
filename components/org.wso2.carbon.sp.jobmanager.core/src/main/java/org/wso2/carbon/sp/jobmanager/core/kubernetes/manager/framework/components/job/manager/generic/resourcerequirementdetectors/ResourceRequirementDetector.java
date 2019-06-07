package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic.resourcerequirementdetectors;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ResourceRequirement;

import java.util.Map;

/**
 * Resource requirements detector for child streaming applications
 * @param <T> Parsed child streaming application
 * @param <R> Runtime of the parsed child streaming application
 */
public interface ResourceRequirementDetector<T,R> {
    /**
     * Generates resource requirements (if any), from the given details
     * @param parsedApp
     * @param appRuntime
     * @param appString
     * @return
     */
    ResourceRequirement generateResourceRequirement(T parsedApp, R appRuntime, String appString);

    /**
     * Generates affinity labels, to abstract in Kubernetes level
     * @return
     */
    Map<String, String> generateAffinityLabels();
}
