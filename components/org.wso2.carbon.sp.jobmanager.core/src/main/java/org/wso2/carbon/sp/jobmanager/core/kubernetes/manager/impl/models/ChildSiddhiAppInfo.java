package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.models;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ResourceRequirement;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.generic.ChildAppInfo;

import java.util.List;

/**
 * Contains details of a child Siddhi app
 */
public class ChildSiddhiAppInfo extends ChildAppInfo {
    public ChildSiddhiAppInfo(String name,
                              String content,
                              List<ResourceRequirement> resourceRequirements,
                              int initialParallelism,
                              boolean isStateful,
                              boolean isReceiver) {
        super(name, content, resourceRequirements, initialParallelism, isStateful, isReceiver);
    }
}
