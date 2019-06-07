package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components;

import org.wso2.carbon.sp.jobmanager.core.SiddhiAppCreator;
import org.wso2.carbon.sp.jobmanager.core.SiddhiTopologyCreator;
import org.wso2.carbon.sp.jobmanager.core.appcreator.DeployableSiddhiQueryGroup;
import org.wso2.carbon.sp.jobmanager.core.appcreator.KafkaSiddhiAppCreator;
import org.wso2.carbon.sp.jobmanager.core.appcreator.SiddhiQuery;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.constants.ProjectConstants;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ResourceRequirement;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.components.job.manager.generic.ChildAppsHandler;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.models.ChildSiddhiAppInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components.resourcerequirementdetectors.SiddhiResourceRequirementDetectorsHolder;
import org.wso2.carbon.sp.jobmanager.core.topology.SiddhiTopology;
import org.wso2.carbon.sp.jobmanager.core.topology.SiddhiTopologyCreatorImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains methods for handling child Siddhi app related actions
 */
public class ChildSiddhiAppsHandler implements ChildAppsHandler<ChildSiddhiAppInfo> {
    @Override
    public List<ChildSiddhiAppInfo> getChildAppInfos(String userDefinedApp, String kafkaIp, String kafkaPort) {
        ProjectConstants.setKafkaURL(kafkaIp, kafkaPort);

        SiddhiTopologyCreator siddhiTopologyCreator = new SiddhiTopologyCreatorImpl();
        SiddhiTopology siddhiTopology = siddhiTopologyCreator.createTopology(userDefinedApp);
        SiddhiAppCreator appCreator = new KafkaSiddhiAppCreator();
        List<DeployableSiddhiQueryGroup> queryGroupList = appCreator.createApps(siddhiTopology);
        return deriveChildSiddhiAppInfos(queryGroupList);
    }

    private List<ChildSiddhiAppInfo> deriveChildSiddhiAppInfos(
            List<DeployableSiddhiQueryGroup> deployableSiddhiQueryGroups) {
        SiddhiResourceRequirementDetectorsHolder resourceRequirementDetectorsHolder =
                new SiddhiResourceRequirementDetectorsHolder();
        resourceRequirementDetectorsHolder.init();
        List<ChildSiddhiAppInfo> childSiddhiApps = new ArrayList<>();
        for (DeployableSiddhiQueryGroup deployableSiddhiQueryGroup : deployableSiddhiQueryGroups) {
            for (SiddhiQuery siddhiQuery : deployableSiddhiQueryGroup.getSiddhiQueries()) {
                // Detect resource requirements
                List<ResourceRequirement> resourceRequirements =
                        resourceRequirementDetectorsHolder
                                .detectResourceRequirements(getSiddhiAppWithoutKafka(siddhiQuery.getApp()));
                childSiddhiApps.add(
                        new ChildSiddhiAppInfo(
                                siddhiQuery.getAppName(),
                                siddhiQuery.getApp(),
                                resourceRequirements,
                                deployableSiddhiQueryGroup.getParallelism(),
                                false,
                                siddhiQuery.isReceiverQuery()));
            }
        }
        return childSiddhiApps;
    }

    private String getSiddhiAppWithoutKafka(String siddhiApp) {
        return siddhiApp
                .replaceAll("@sink\\(type='kafka'","--@sink(type='kafka'")
                .replaceAll("@source\\(type='kafka'", "--@source(type='kafka'");
    }
}
