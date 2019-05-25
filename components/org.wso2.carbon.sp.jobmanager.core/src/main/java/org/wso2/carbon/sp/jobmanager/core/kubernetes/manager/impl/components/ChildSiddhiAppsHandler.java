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
    @Override
    public List<ChildSiddhiAppInfo> getChildAppInfos(String userDefinedApp) {
        List<ChildSiddhiAppInfo> childApps = getHardCodedChildSiddhiApps();
        return detectSiddhiQueryElements(childApps);
//        return getHardCodedChildSiddhiApps(); // TODO fix below

//        SiddhiTopologyCreator siddhiTopologyCreator = new SiddhiTopologyCreatorImpl();
//        SiddhiTopology siddhiTopology = siddhiTopologyCreator.createTopology(userDefinedApp);

        // TODO fragile with Kafka - following 2 lines. uncommented in K8s's image
        // TODO without the following, the 'DeployableSiddhiApp' won't be created!
//        SiddhiAppCreator appCreator = new KafkaSiddhiAppCreator();
//        List<DeployableSiddhiQueryGroup> queryGroupList = appCreator.createApps(siddhiTopology);

//        return Collections.emptyList(); // TODO remove
//
//        return extractSiddhiQueryElements(siddhiTopology.getQueryGroupList()); // TODO uncomment
    }

    // TODO remove
    private List<ChildSiddhiAppInfo> getHardCodedChildSiddhiApps() {
        List<ChildSiddhiAppInfo> childSiddhiAppInfos = new ArrayList<>();
        String hardCodedApp1 = "@App:name('simple-group-1-1') \n" +
                "@Source(type='http',\n" +
                "\treceiver.url='http://0.0.0.0:8006/productionStream',\n" +
                "\tbasic.auth.enabled='false',\n" +
                "\t@map(type='json')) \n" +
                "define stream allStream(name string, amount double);\n" +
                "@sink(type='log')\n" +
                "define stream greaterThanFifty(name string, amount double);\n" +
                "@info(name='greaterThanFifty')\n" +
                "\n" +
                "from allStream[amount > 50]\n" +
                "select *\n" +
                "insert into greaterThanFifty;";
        ChildSiddhiAppInfo childSiddhiAppInfo1 = new ChildSiddhiAppInfo(
                "simple-group-1-1",
                hardCodedApp1,
                null,
                1,
                false,
                false);
        String hardCodedApp2 = "@App:name('simple-group-2-1') \n" +
                "@Source(type='http',\n" +
                "\treceiver.url='http://0.0.0.0:8006/productionStream',\n" +
                "\tbasic.auth.enabled='false',\n" +
                "\t@map(type='json')) \n" +
                "define stream allStream(name string, amount double);\n" +
                "@store(type='rdbms',\n" +
                "\tjdbc.url='jdbc:mysql//localhost:3306/simple',\n" +
                "\tusername='root',\n" +
                "\tpassword='',\n" +
                "\tjdbc.driver.name='com.mysql.jdbc.Driver')\n" +
                "define table lessThanFifty(name string, amount double);\n" +
                "@info(name='lessThanFifty')\n" +
                "\n" +
                "from allStream[amount < 50]\n" +
                "select *\n" +
                "insert into lessThanFifty;";
        ChildSiddhiAppInfo childSiddhiAppInfo2 = new ChildSiddhiAppInfo(
                "simple-group-2-1",
                hardCodedApp2,
                null,
                1,
                false,
                false);
        childSiddhiAppInfos.add(childSiddhiAppInfo1);
        childSiddhiAppInfos.add(childSiddhiAppInfo2);
        return childSiddhiAppInfos;
    }

    public static void main(String[] args) {
        ChildSiddhiAppsHandler childSiddhiAppsHandler = new ChildSiddhiAppsHandler();
        List<ChildSiddhiAppInfo> childApps = childSiddhiAppsHandler.getHardCodedChildSiddhiApps();
        List<ChildSiddhiAppInfo> resourceApps = childSiddhiAppsHandler.detectSiddhiQueryElements(childApps);
    }

    private List<ChildSiddhiAppInfo> detectSiddhiQueryElements(List<ChildSiddhiAppInfo> childSiddhiApps) {
        SiddhiResourceRequirementDetectorsHolder siddhiResourceRequirementDetectorsHolder =
                new SiddhiResourceRequirementDetectorsHolder();
        siddhiResourceRequirementDetectorsHolder.init();
        List<ChildSiddhiAppInfo> childSiddhiAppInfos = new ArrayList<>();

        for (ChildSiddhiAppInfo childSiddhiAppInfo : childSiddhiApps) {
            List<ResourceRequirement> resourceRequirements =
                    siddhiResourceRequirementDetectorsHolder.detectResourceRequirements(childSiddhiAppInfo.getContent());
            childSiddhiAppInfos.add(
                    new ChildSiddhiAppInfo(
                            childSiddhiAppInfo.getName(),
                            childSiddhiAppInfo.getContent(),
                            resourceRequirements,
                            1,
                            false,
                            false));
        }

        return childSiddhiAppInfos;
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
