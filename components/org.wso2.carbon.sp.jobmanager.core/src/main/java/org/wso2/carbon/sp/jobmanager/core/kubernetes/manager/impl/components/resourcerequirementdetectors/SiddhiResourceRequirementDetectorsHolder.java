package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components.resourcerequirementdetectors;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ResourceRequirement;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic.resourcerequirementdetectors.ResourceRequirementDetectorsHolder;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components.resourcerequirementdetectors.rdbmsstore.MySQLStoreDetector;
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
public class SiddhiResourceRequirementDetectorsHolder implements ResourceRequirementDetectorsHolder {
    private List<SiddhiResourceRequirementDetector> siddhiResourceRequirementDetectors = new ArrayList<>();

    /**
     * Registers known requirement detectors
     */
    public void init() {
        siddhiResourceRequirementDetectors.add(new MySQLStoreDetector());
    }

    /**
     * Detects resource requirements by using all the registered resource requirement detectors
     *
     * @param siddhiAppString Siddhi app string
     * @return Resource requirements
     */
    public List<ResourceRequirement> detectResourceRequirements(String siddhiAppString) {
        SiddhiApp siddhiApp = SiddhiCompiler.parse(siddhiAppString);
        SiddhiAppRuntime siddhiAppRuntime = new SiddhiManager().createSiddhiAppRuntime(siddhiApp);
        List<ResourceRequirement> resourceRequirements = new ArrayList<>();
        for (SiddhiResourceRequirementDetector siddhiResourceRequirementDetector : siddhiResourceRequirementDetectors) {
            resourceRequirements.add(
                    siddhiResourceRequirementDetector.generateResourceRequirement(
                            siddhiApp, siddhiAppRuntime, siddhiAppString));
        }
        return resourceRequirements;
    }
}
