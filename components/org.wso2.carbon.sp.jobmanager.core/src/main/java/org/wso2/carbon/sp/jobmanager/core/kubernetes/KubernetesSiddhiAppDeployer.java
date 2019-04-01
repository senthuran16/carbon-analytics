package org.wso2.carbon.sp.jobmanager.core.kubernetes;

import com.google.gson.Gson;
import feign.FeignException;
import feign.Response;
import org.apache.log4j.Logger;
import org.wso2.carbon.sp.jobmanager.core.api.ResourceServiceFactory;
import org.wso2.carbon.sp.jobmanager.core.appcreator.SiddhiQuery;
import org.wso2.carbon.sp.jobmanager.core.impl.utils.Constants;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.ChildSiddhiAppInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.DeploymentInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.WorkerPodInfo;
import org.wso2.carbon.sp.jobmanager.core.model.ResourceNode;
import org.wso2.carbon.sp.jobmanager.core.util.HTTPSClientUtil;

import java.util.Arrays;
import java.util.List;

/**
 * This class gives the details of the deployed siddhi application's details.
 */
public class KubernetesSiddhiAppDeployer {
    private static final Logger LOG = Logger.getLogger(KubernetesSiddhiAppDeployer.class);

    public static void exec() { // TODO remove
        WorkerPodInfo workerPodInfo = new WorkerPodInfo("test-app-group-1-1", "10.36.1.47", "test-app-group-1-1");
        String hardCodedApp = "@App:name('test-app-group-1-1') \n" +
//                "@source(type='kafka', topic.list='test-app.InputStreamOne', group.id='test-app-group-1-0', threading.option='single.thread', bootstrap.servers='localhost:9092', @map(type='xml'))" +
                "define stream InputStreamOne (name string);\n" +
                "@sink(type='log')\n" +
                "define stream LogStreamOne(name string);\n" +
                "@info(name='query1')\n" +
                "\n" +
                "from InputStreamOne\n" +
                "select *\n" +
                "insert into LogStreamOne;";


        ChildSiddhiAppInfo childSiddhiAppInfo =
                new ChildSiddhiAppInfo("test-app-group-1-1", hardCodedApp, 1, false, false);
//        deploy(workerPodInfo, childSiddhiAppInfo);
        System.out.println("Deployed");

    }

    public static boolean deploy(DeploymentInfo deployment) {
        Response resourceResponse = null;
        try {
            resourceResponse = ResourceServiceFactory
                    .getResourceHttpsClient(
                            Constants.PROTOCOL + HTTPSClientUtil.generateURLHostPort(
                                    deployment.getWorkerPodInfo().getIp(), "9443"),
                            "admin", "admin") // TODO remove hardcoded
                    .postSiddhiApp(deployment.getChildSiddhiAppInfo().getContent());

            if (resourceResponse != null) {
                if (resourceResponse.status() == 200) {
                    return true;
                }
            }

            return false;
        } catch (FeignException e) {
            // TODO log if isDebugEnabled
            System.out.println("Failed to create deployment: " + deployment);
            return false;
        } finally {
            if (resourceResponse != null) {
                resourceResponse.close();
            }
        }
    }
}
