package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.WorkerPodInfo;

public interface GenericWorkerPodsMonitor {
    double getMetrics(WorkerPodInfo pod);
}
