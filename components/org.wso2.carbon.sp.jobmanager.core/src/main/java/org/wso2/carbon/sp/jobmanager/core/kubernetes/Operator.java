package org.wso2.carbon.sp.jobmanager.core.kubernetes;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.ChildSiddhiAppInfo;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.TimerTask;

/**
 * Handles the deployment and dynamic scaling in a Kubernetes cluster
 */
public class Operator extends TimerTask {
    KubernetesClient kubernetesClient;
    ChildSiddhiAppsHandler childSiddhiAppsHandler;
    DeploymentManager deploymentManager;

    public Operator(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
        this.childSiddhiAppsHandler = new ChildSiddhiAppsHandler();
        this.deploymentManager = new DeploymentManager(kubernetesClient);
    }

    @Override
    public void run() {

    }

    public static void main(String[] args) throws FileNotFoundException {
        Operator operator = new Operator(new DefaultKubernetesClient());

        String siddhiApp = "@App:name('test-app')\n" +
                "@App:description('Description of the plan')\n" +
                "\n" +
                "define stream InputStreamOne (name string);\n" +
                "define stream InputStreamTwo (name string);\n" +
                "\n" +
                "@sink(type='log')\n" +
                "define stream LogStreamOne(name string);\n" +
                "\n" +
                "@sink(type='log')\n" +
                "define stream LogStreamTwo(name string);\n" +
                "\n" +
                "@info(name='query1')\n" +
                "@dist(execGroup='group-1')\n" +
                "from InputStreamOne\n" +
                "select *\n" +
                "insert into LogStreamOne;\n" +
                "\n" +
                "@info(name='query2')\n" +
                "@dist(execGroup='group-2' ,parallel ='2')\n" +
                "from InputStreamTwo\n" +
                "select *\n" +
                "insert into LogStreamTwo;";

        List<ChildSiddhiAppInfo> childSiddhiAppInfos =
                operator.childSiddhiAppsHandler.getChildSiddhiAppInfos(siddhiApp);
        operator.deploymentManager.createChildSiddhiAppDeployments(childSiddhiAppInfos);
        operator.deploymentManager.updateDeployments();
        System.out.println("Success!");
    }
}
