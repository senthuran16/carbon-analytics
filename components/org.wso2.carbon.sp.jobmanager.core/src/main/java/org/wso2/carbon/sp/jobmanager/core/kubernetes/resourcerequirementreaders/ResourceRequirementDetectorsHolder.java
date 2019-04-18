package org.wso2.carbon.sp.jobmanager.core.kubernetes.resourcerequirementreaders;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.ResourceRequirement;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.resourcerequirementreaders.rdbmsstore.MySQLStoreDetector;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds resource requirement detectors.
 * Each newly implemented resource requirement detector should be added in the init() method
 */
public class ResourceRequirementDetectorsHolder {
    private static List<ResourceRequirementDetector> resourceRequirementDetectors = new ArrayList<>();

    /**
     * Registers known requirement detectors
     */
    public static void init() {
        resourceRequirementDetectors.add(new MySQLStoreDetector());
    }

    /**
     * Detects resource requirements by using all the registered resource requirement detectors
     *
     * @param siddhiAppString Siddhi app string
     * @return Resource requirements
     */
    public static List<ResourceRequirement> detectResourceRequirements(String siddhiAppString) {
        SiddhiApp siddhiApp = SiddhiCompiler.parse(siddhiAppString);
        SiddhiAppRuntime siddhiAppRuntime = new SiddhiManager().createSiddhiAppRuntime(siddhiApp);
        List<ResourceRequirement> resourceRequirements = new ArrayList<>();
        for (ResourceRequirementDetector resourceRequirementDetector : resourceRequirementDetectors) {
            resourceRequirements.add(
                    resourceRequirementDetector.generateResourceRequirement(
                            siddhiApp, siddhiAppRuntime, siddhiAppString));
        }
        return resourceRequirements;
    }
}
