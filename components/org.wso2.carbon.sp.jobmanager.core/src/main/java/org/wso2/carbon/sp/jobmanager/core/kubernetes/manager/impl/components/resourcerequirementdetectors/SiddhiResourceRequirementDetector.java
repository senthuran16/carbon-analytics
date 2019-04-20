package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components.resourcerequirementdetectors;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic.resourcerequirementdetectors.ResourceRequirementDetector;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.query.api.SiddhiApp;

public abstract class SiddhiResourceRequirementDetector
        implements ResourceRequirementDetector<SiddhiApp, SiddhiAppRuntime> {

}
