package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ManagerServiceInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.generic.ChildAppInfo;

import java.util.List;

/**
 * Handles child streaming applications
 * @param <T> Type of your Stream Processor's child streaming application
 */
public interface ChildAppsHandler <T extends ChildAppInfo> {
    /**
     * Derives child streaming applications from the given user defined streaming application,
     * and intermediate messaging system
     * @param userDefinedApp
     * @param kafkaIp
     * @param kafkaPort
     * @return
     */
    List<T> getChildAppInfos(String userDefinedApp, String kafkaIp, String kafkaPort);
}
