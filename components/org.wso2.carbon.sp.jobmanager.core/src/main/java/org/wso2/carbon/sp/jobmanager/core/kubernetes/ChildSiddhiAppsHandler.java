package org.wso2.carbon.sp.jobmanager.core.kubernetes;

import org.wso2.carbon.sp.jobmanager.core.SiddhiAppCreator;
import org.wso2.carbon.sp.jobmanager.core.SiddhiTopologyCreator;
import org.wso2.carbon.sp.jobmanager.core.appcreator.AbstractSiddhiAppCreator;
import org.wso2.carbon.sp.jobmanager.core.appcreator.DeployableSiddhiQueryGroup;
import org.wso2.carbon.sp.jobmanager.core.appcreator.KafkaSiddhiAppCreator;
import org.wso2.carbon.sp.jobmanager.core.bean.DeploymentConfig;
import org.wso2.carbon.sp.jobmanager.core.bean.ZooKeeperConfig;
import org.wso2.carbon.sp.jobmanager.core.impl.DistributionManagerServiceImpl;
import org.wso2.carbon.sp.jobmanager.core.internal.ServiceComponent;
import org.wso2.carbon.sp.jobmanager.core.internal.ServiceDataHolder;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.ChildSiddhiAppInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.ResourceRequirement;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.resourcerequirementreaders.ResourceRequirementDetectorsHolder;
import org.wso2.carbon.sp.jobmanager.core.topology.SiddhiQueryGroup;
import org.wso2.carbon.sp.jobmanager.core.topology.SiddhiTopology;
import org.wso2.carbon.sp.jobmanager.core.topology.SiddhiTopologyCreatorImpl;
import org.wso2.carbon.stream.processor.core.internal.StreamProcessorDataHolder;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.SiddhiElement;
import org.wso2.siddhi.query.api.execution.ExecutionElement;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Contains methods for handling child Siddhi app related actions
 */
public class ChildSiddhiAppsHandler {
    public static void main(String[] args) {
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
        List<ChildSiddhiAppInfo> test = new ChildSiddhiAppsHandler().getChildSiddhiAppInfos(siddhiApp);
        System.out.println("SUCCESS");
    }

    public List<ChildSiddhiAppInfo> getChildSiddhiAppInfos(String userDefinedSiddhiApp) {
//        return getHardCodedChildSiddhiApps(); // TODO fix below

        SiddhiTopologyCreator siddhiTopologyCreator = new SiddhiTopologyCreatorImpl();
        SiddhiTopology siddhiTopology = siddhiTopologyCreator.createTopology(userDefinedSiddhiApp);

        // TODO fragile with Kafka - following 2 lines. uncommented in K8s's image
        // TODO without the following, the 'DeployableSiddhiApp' won't be created!
        SiddhiAppCreator appCreator = new KafkaSiddhiAppCreator();
        List<DeployableSiddhiQueryGroup> queryGroupList = appCreator.createApps(siddhiTopology);

//        return Collections.emptyList(); // TODO remove
//
        return extractSiddhiQueryElements(siddhiTopology.getQueryGroupList()); // TODO uncomment

        // TODO old code below
//        SiddhiTopologyCreator siddhiTopologyCreator = new SiddhiTopologyCreatorImpl();
//        SiddhiTopology siddhiTopology = siddhiTopologyCreator.createTopology(userDefinedSiddhiApp);
//
//        // TODO fragile with Kafka - following 2 lines. uncommented in K8s's image
//        // TODO without the following, the 'DeployableSiddhiApp' won't be created!
//        SiddhiAppCreator appCreator = new KafkaSiddhiAppCreator();
//        List<DeployableSiddhiQueryGroup> queryGroupList = appCreator.createApps(siddhiTopology);
//
////        return Collections.emptyList(); // TODO remove
////
//        return extractSiddhiQueryElements(siddhiTopology.getQueryGroupList()); // TODO uncomment
    }

    private List<ChildSiddhiAppInfo> extractSiddhiQueryElements(List<SiddhiQueryGroup> siddhiQueryGroups) {
        ResourceRequirementDetectorsHolder.init();
        List<ChildSiddhiAppInfo> childSiddhiAppInfos = new ArrayList<>();
        for (SiddhiQueryGroup siddhiQueryGroup : siddhiQueryGroups) {
            String siddhiQueryGroupName = siddhiQueryGroup.getName();
            // TODO in existing impl, query group is adhered to user config. In mine, each member is a new Query Group
            int queryIndex = 0;
            for (String query : siddhiQueryGroup.getQueryList()) {
                List<ResourceRequirement> resourceRequirements =
                        ResourceRequirementDetectorsHolder.detectResourceRequirements(query);
                childSiddhiAppInfos.add(
                        new ChildSiddhiAppInfo(
                                siddhiQueryGroupName + "-" + queryIndex++,
                                query,
                                resourceRequirements,//TODO uncomment
//                                null, // TODO remove when appcreator.createApps() works
                                siddhiQueryGroup.getParallelism(),
                                false, // TODO check isStateful properly
                                siddhiQueryGroup.isReceiverQueryGroup()));
            }
        }

        return childSiddhiAppInfos;
    }

    private boolean isChildAppStateful(String childApp) {
        SiddhiApp siddhiApp = SiddhiCompiler.parse(childApp);
        for (ExecutionElement executionElement : siddhiApp.getExecutionElementList()) {
            if (isSiddhiElementStateful(executionElement)) {
                return true;
            }
        }
        return false;
    }

    private boolean isSiddhiElementStateful(ExecutionElement executionElement) {
        if (executionElement instanceof Query) {
            InputStream queryInputStream = ((Query)executionElement).getInputStream();
            return queryInputStream instanceof StateInputStream;
        }
        return false;
    }

//    private List<ChildSiddhiAppInfo> getHardCodedChildSiddhiApps() { // TODO remove when finalized
//        List<ChildSiddhiAppInfo> childSiddhiAppInfos = new ArrayList<>();
//        String hardCodedApp1 = "@App:name('test-app-group-1-1') \n" +
////                "@source(type='kafka', topic.list='test-app.InputStreamOne', group.id='test-app-group-1-0', threading.option='single.thread', bootstrap.servers='localhost:9092', @map(type='xml'))" +
//                "define stream InputStreamOne (name string);\n" +
//                "@sink(type='log')\n" +
//                "define stream LogStreamOne(name string);\n" +
//                "@info(name='query1')\n" +
//                "\n" +
//                "from InputStreamOne\n" +
//                "select *\n" +
//                "insert into LogStreamOne;";
//
//        String hardCodedApp2 = "@App:name('test-app-group-2-1') \n" +
////                "@source(type='kafka', topic.list='test-app.InputStreamTwo', group.id='test-app-group-2', threading.option='single.thread', bootstrap.servers='localhost:9092', @map(type='xml'))" +
//                "define stream InputStreamTwo (name string);\n" +
//                "@sink(type='log')\n" +
//                "define stream LogStreamTwo(name string);\n" +
//                "@info(name='query2')\n" +
//                "\n" +
//                "from InputStreamTwo\n" +
//                "select *\n" +
//                "insert into LogStreamTwo;";
//        childSiddhiAppInfos.add(
//                new ChildSiddhiAppInfo(
//                        "test-app-group-1-1",
//                        hardCodedApp1,
//                        null,
//                        1,
//                        isChildAppStateful(hardCodedApp1),
//                        false));
//        childSiddhiAppInfos.add(
//                new ChildSiddhiAppInfo(
//                        "test-app-group-2-1",
//                        hardCodedApp2,
//                        null,
//                        2,
//                        isChildAppStateful(hardCodedApp1),
//                        false));
//        return childSiddhiAppInfos;
//    }
}
