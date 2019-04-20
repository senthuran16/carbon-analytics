package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components;

import feign.FeignException;
import feign.Response;
import org.apache.log4j.Logger;
import org.wso2.carbon.sp.jobmanager.core.api.ResourceServiceFactory;
import org.wso2.carbon.sp.jobmanager.core.impl.utils.Constants;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.DeploymentInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ResourceRequirement;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic.KubernetesChildAppDeployer;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components.resourcerequirementdetectors.SiddhiResourceRequirementDetectorsHolder;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.models.ChildSiddhiAppInfo;
import org.wso2.carbon.sp.jobmanager.core.util.HTTPSClientUtil;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.util.List;

/**
 * This class gives the details of the deployed siddhi application's details.
 */
public class KubernetesSiddhiAppDeployer implements KubernetesChildAppDeployer {
    private static final Logger LOG = Logger.getLogger(KubernetesSiddhiAppDeployer.class);

    public static void main(String[] args) {
        String siddhiAppString = "define stream insertSweetProductionStream (name string, amount double);\n" +
                "\n" +
                "@Store(type=\"rdbms\",\n" +
                "       jdbc.url=\"jdbc:mysql://localhost:3306/production\",\n" +
                "       username=\"root\",\n" +
                "       password=\"\" ,\n" +
                "       jdbc.driver.name=\"com.mysql.jdbc.Driver\")\n" +
                "--@PrimaryKey(\"name\")\n" +
                "@index(\"amount\")\n" +
                "define table SweetProductionTable (name string, amount double);\n" +
                "\n" +
                "@info(name='query1')\n" +
                "from insertSweetProductionStream\n" +
                "insert into SweetProductionTable;";
        SiddhiApp siddhiApp = SiddhiCompiler.parse(siddhiAppString);
        SiddhiResourceRequirementDetectorsHolder holder = new SiddhiResourceRequirementDetectorsHolder();
        holder.init();
        List<ResourceRequirement> resourceRequirements = holder.detectResourceRequirements(siddhiAppString);

        ChildSiddhiAppInfo siddhiAppInfo =
                new ChildSiddhiAppInfo("dummy-group-2-1",
                        "@App:name('dummy-group-2-1') \n" +
                                "@source(type='kafka', topic.list='dummy.inStream', group.id='dummy-group-2-0', threading.option='single.thread', bootstrap.servers='localhost:9092', @map(type='xml')) \n" +
                                "define stream inStream (message string, value int);\n" +
                                "@store(type = 'rdbms', jdbc.url = \"jdbc:mysql://localhost:3306/production\", username = \"root\", password = \"\", jdbc.driver.name = \"com.mysql.jdbc.Driver\")\n" +
                                "define table myTable (message string, value int);\n" +
                                "@info(name = 'greaterThanHundred')\n" +
                                "\n" +
                                "from inStream[value > 100] \n" +
                                "select * \n" +
                                "insert into myTable;\n",
                        resourceRequirements,
                        1,
                        false,
                        false
                        );

        Object o = null;

    }

    @Override
    public boolean deploy(DeploymentInfo deployment) {
        Response resourceResponse = null;
        try {
            resourceResponse = ResourceServiceFactory
                    .getResourceHttpsClient(
                            Constants.PROTOCOL + HTTPSClientUtil.generateURLHostPort(
                                    deployment.getWorkerPodInfo().getIp(), "9443"),
                            "admin", "admin") // TODO remove hardcoded
                    .postSiddhiApp(deployment.getChildAppInfo().getContent());
            if (resourceResponse != null) {
                if (resourceResponse.status() == 200) {
                    return true;
                }
            }
            return false;
        } catch (FeignException e) {
            System.out.println("Failed to create deployment: " + deployment);
            return false;
        } finally {
            if (resourceResponse != null) {
                resourceResponse.close();
            }
        }
    }
}
