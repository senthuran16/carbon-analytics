package org.wso2.carbon.sp.jobmanager.core.kubernetes.resourcerequirementreaders;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.ResourceRequirement;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.query.api.SiddhiApp;

import java.util.Map;

/**
 * Describes the functionality of a resource requirement detector.
 * This interface must be implemented by the classes,
 * that can generate labels based on special resource requirements of a child Siddhi app
 */
public interface ResourceRequirementDetector {
    ResourceRequirement generateResourceRequirement(SiddhiApp siddhiApp,
                                                                    SiddhiAppRuntime siddhiAppRuntime,
                                                                    String siddhiAppString);

    Map<String, String> generateAffinityLabels();
}
