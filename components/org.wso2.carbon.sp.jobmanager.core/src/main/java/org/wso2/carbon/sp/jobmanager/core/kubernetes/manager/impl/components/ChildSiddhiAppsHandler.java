package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components;

import org.wso2.carbon.sp.jobmanager.core.SiddhiTopologyCreator;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ResourceRequirement;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic.ChildAppsHandler;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.models.ChildSiddhiAppInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components.resourcerequirementdetectors.SiddhiResourceRequirementDetectorsHolder;
import org.wso2.carbon.sp.jobmanager.core.topology.SiddhiQueryGroup;
import org.wso2.carbon.sp.jobmanager.core.topology.SiddhiTopology;
import org.wso2.carbon.sp.jobmanager.core.topology.SiddhiTopologyCreatorImpl;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.execution.ExecutionElement;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains methods for handling child Siddhi app related actions
 */
public class ChildSiddhiAppsHandler implements ChildAppsHandler<ChildSiddhiAppInfo> {
    public static void main(String[] args) {
        String siddhiApp = "@App:name('dummy-group-2-1') \n" +
//                "@source(type='kafka', topic.list='dummy.inStream', group.id='dummy-group-2-0', threading.option='single.thread', bootstrap.servers='localhost:9092', @map(type='xml')) \n" +
                "define stream inStream (message string, value int);\n" +
                "@store(type = 'rdbms', jdbc.url = \"jdbc:mysql://localhost:3306/production\", username = \"root\", password = \"\", jdbc.driver.name = \"com.mysql.jdbc.Driver\")\n" +
                "define table myTable (message string, value int);\n" +
                "@info(name = 'greaterThanHundred')\n" +
                "from inStream[value > 100] \n" +
                "select * \n" +
                "insert into myTable;\n";
        List<ChildSiddhiAppInfo> childSiddhiAppInfos = new ChildSiddhiAppsHandler().getChildAppInfos(siddhiApp);
        System.out.println("SUCCESS");
    }

    @Override
    public List<ChildSiddhiAppInfo> getChildAppInfos(String userDefinedApp) {
        //        return getHardCodedChildSiddhiApps(); // TODO fix below

        SiddhiTopologyCreator siddhiTopologyCreator = new SiddhiTopologyCreatorImpl();
        SiddhiTopology siddhiTopology = siddhiTopologyCreator.createTopology(userDefinedApp);

        // TODO fragile with Kafka - following 2 lines. uncommented in K8s's image
        // TODO without the following, the 'DeployableSiddhiApp' won't be created!
//        SiddhiAppCreator appCreator = new KafkaSiddhiAppCreator();
//        List<DeployableSiddhiQueryGroup> queryGroupList = appCreator.createApps(siddhiTopology);

//        return Collections.emptyList(); // TODO remove
//
        return extractSiddhiQueryElements(siddhiTopology.getQueryGroupList()); // TODO uncomment
    }

    private List<ChildSiddhiAppInfo> extractSiddhiQueryElements(List<SiddhiQueryGroup> siddhiQueryGroups) {
        SiddhiResourceRequirementDetectorsHolder siddhiResourceRequirementDetectorsHolder =
                new SiddhiResourceRequirementDetectorsHolder();
        siddhiResourceRequirementDetectorsHolder.init();
        List<ChildSiddhiAppInfo> childSiddhiAppInfos = new ArrayList<>();
        for (SiddhiQueryGroup siddhiQueryGroup : siddhiQueryGroups) {
            String siddhiQueryGroupName = siddhiQueryGroup.getName();
            // TODO in existing impl, query group is adhered to user config. In mine, each member is a new Query Group
            int queryIndex = 0;
            for (String query : siddhiQueryGroup.getQueryList()) {
                List<ResourceRequirement> resourceRequirements =
                        siddhiResourceRequirementDetectorsHolder.detectResourceRequirements(query);
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
