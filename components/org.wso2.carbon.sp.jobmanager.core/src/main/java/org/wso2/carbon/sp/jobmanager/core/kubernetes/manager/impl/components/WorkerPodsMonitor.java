package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import feign.FeignException;
import org.apache.log4j.Logger;
import org.wso2.carbon.sp.jobmanager.core.api.ResourceServiceFactory;
import org.wso2.carbon.sp.jobmanager.core.impl.utils.Constants;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.WorkerPodInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic.GenericWorkerPodsMonitor;
import org.wso2.carbon.sp.jobmanager.core.util.HTTPSClientUtil;

/**
 * Monitors the worker pods and provides their metrics information
 */
public class WorkerPodsMonitor implements GenericWorkerPodsMonitor {
    private static final Logger LOG = Logger.getLogger(WorkerPodsMonitor.class);

    @Override
    public double getMetrics(WorkerPodInfo pod) {
        feign.Response resourceResponse = null;
        try {
            resourceResponse = ResourceServiceFactory
                    .getResourceHttpsClient(
                            Constants.PROTOCOL + HTTPSClientUtil.generateURLHostPort(pod.getIp(), "9443"),
                            "admin", "admin")
                    .getWorkerPodMetrics();

            if (resourceResponse != null) {
                if (resourceResponse.status() == 200) {
                    return Double.valueOf(
                            new Gson().fromJson(
                                    new Gson().fromJson(
                                            resourceResponse.body().toString(), JsonObject.class)
                                            .get("workerMetrics"), JsonObject.class)
                                    .get("loadAverage").toString());
                }
            }
            return 0;
        } catch (FeignException e) {
            return 0;
        } finally {
            if (resourceResponse != null) {
                resourceResponse.close();
            }
        }
    }
}
