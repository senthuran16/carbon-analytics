package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.WorkerPodInfo;

/**
 * Monitors Worker pods
 */
public interface GenericWorkerPodsMonitor {
    /**
     * Gets metrics of a Worker pod
     * @param pod
     * @return
     */
    double getMetrics(WorkerPodInfo pod);
}
