package org.wso2.carbon.sp.jobmanager.core.kubernetes;

import feign.FeignException;
import feign.Response;
import org.apache.log4j.Logger;
import org.wso2.carbon.sp.jobmanager.core.api.ResourceServiceFactory;
import org.wso2.carbon.sp.jobmanager.core.impl.utils.Constants;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.DeploymentInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.ResourceRequirement;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.resourcerequirementreaders.ResourceRequirementDetectorsHolder;
import org.wso2.carbon.sp.jobmanager.core.util.HTTPSClientUtil;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.util.List;

/**
 * This class gives the details of the deployed siddhi application's details.
 */
public class KubernetesSiddhiAppDeployer {
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
        ResourceRequirementDetectorsHolder.init();
        List<ResourceRequirement> resourceRequirements =
                ResourceRequirementDetectorsHolder.detectResourceRequirements(siddhiAppString);

        Object o = null;

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
            System.out.println("Failed to create deployment: " + deployment);
            return false;
        } finally {
            if (resourceResponse != null) {
                resourceResponse.close();
            }
        }
    }
}
