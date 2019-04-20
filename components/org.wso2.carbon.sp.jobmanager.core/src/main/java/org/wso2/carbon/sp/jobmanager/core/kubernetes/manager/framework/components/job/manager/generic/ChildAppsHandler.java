package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.generic.ChildAppInfo;

import java.util.List;

public interface ChildAppsHandler <T extends ChildAppInfo> {
    List<T> getChildAppInfos(String userDefinedApp);
}
