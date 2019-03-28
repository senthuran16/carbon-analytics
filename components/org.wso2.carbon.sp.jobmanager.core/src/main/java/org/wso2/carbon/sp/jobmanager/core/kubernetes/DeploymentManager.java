package org.wso2.carbon.sp.jobmanager.core.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.exception.KubernetesDeploymentManagerException;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.ChildSiddhiAppInfo;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.models.WorkerPodInfo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles creating, scheduling and scaling aspects of a Kubernetes deployment
 */
public class DeploymentManager {
    private static final String WORKER_SERVICE_TEMPLATE_ABSOLUTE_FILE_PATH =
            "/home/senthuran/CodeIIT/FYP/impl/resources/v4/runnerService.yaml"; // TODO remove when done
    private static final String WORKER_DEPLOYMENT_TEMPLATE_ABSOLUTE_FILE_PATH =
            "/home/senthuran/CodeIIT/FYP/impl/resources/v4/runnerDeployment.yaml"; // TODO remove when done

    private static final String WORKER_DEPLOYMENT_RESOURCE_PATH =
            "resources/workerDeployment.yaml";
    private static final String WORKER_SERVICE_RESOURCE_PATH =
            "resources/workerService.yaml";


    private static final String WORKERS_NAMESPACE = "wso2"; // TODO create this namespace programmatically first

    private static final String SIDDHI_APP_LABEL_KEY = "siddhi-app";
    private static final String APP_LABEL_KEY = "app";
    private static final String NODE_LABEL_KEY = "node";
    private static final String PATTERN_LABEL_KEY = "pattern";
    private static final String PATTERN_LABEL_VALUE = "wso2sp-pattern-distributed";

    private Map<String, ChildSiddhiAppInfo> childSiddhiApps;
    private KubernetesClient kubernetesClient;
    private List<WorkerPodInfo> availableWorkerPods;

    public DeploymentManager(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
        availableWorkerPods = new ArrayList<>();
        childSiddhiApps = new HashMap<>();
    }

    ////
    public void updateDeployments() { // TODO implement
        List<WorkerPodInfo> latestPods = getLatestPods();
        System.out.println(latestPods.size() + " new Pods were found"); // TODO log
        System.out.println(latestPods); // TODO log
        System.out.println(""); // TODO log

        Map<WorkerPodInfo, ChildSiddhiAppInfo> deployableSiddhiApps = getDeployableSiddhiAppInfos(latestPods);
        for (WorkerPodInfo workerPodInfo : deployableSiddhiApps.keySet()) {
            KubernetesSiddhiAppDeployer.deploy(workerPodInfo, deployableSiddhiApps.get(workerPodInfo)); // TODO test
            System.out.println(
                    "Deployed Siddhi app: " + deployableSiddhiApps.get(workerPodInfo.getChildSiddhiAppName()) +
                    " in Worker Pod: " + workerPodInfo.getName());
        }
        System.out.println("Deployment Updated"); // TODO log
    }

    private List<WorkerPodInfo> getLatestPods() {
        List<WorkerPodInfo> latestPods = getAvailableWorkerPods();
        latestPods.removeAll(this.availableWorkerPods);

        return latestPods;
    }

    /**
     * Gets information about all the available Pods
     * @return  List of Pod details
     */
    public List<WorkerPodInfo> getAvailableWorkerPods() { // TODO make this private
        List<Pod> pods = kubernetesClient.pods().list().getItems();
        List<WorkerPodInfo> workerPodInfos = new ArrayList<>();
        for (Pod pod : pods) {
            if (pod.getMetadata() != null &&
                    pod.getMetadata().getLabels() != null &&
//                    pod.getMetadata().getLabels().containsKey(SIDDHI_APP_LABEL_KEY)) {
                    pod.getMetadata().getLabels().containsKey(APP_LABEL_KEY)) {
                workerPodInfos.add(
                        new WorkerPodInfo(
                                pod.getMetadata().getName(),
                                pod.getStatus().getPodIP(),
                                pod.getMetadata().getLabels().get(SIDDHI_APP_LABEL_KEY)));
            }
        }

        return workerPodInfos;
    }

    private Map<WorkerPodInfo, ChildSiddhiAppInfo> getDeployableSiddhiAppInfos(List<WorkerPodInfo> newPods) {
        Map<WorkerPodInfo, ChildSiddhiAppInfo> deployableChildSiddhiApps = new HashMap<>(); // TODO confirm
        for(WorkerPodInfo workerPodInfo : newPods) {
            deployableChildSiddhiApps.put(workerPodInfo, getChildSiddhiAppInfo(workerPodInfo));
        }
        return deployableChildSiddhiApps;
    }

    private ChildSiddhiAppInfo getChildSiddhiAppInfo(WorkerPodInfo workerPodInfo) {
        ChildSiddhiAppInfo childSiddhiAppInfo = childSiddhiApps.get(getChildSiddhiAppName(workerPodInfo));
        if (childSiddhiAppInfo != null) {
            return childSiddhiAppInfo;
        }
        throw new KubernetesDeploymentManagerException(
                "Unable to find child Siddhi app for worker pod: " + workerPodInfo);
    }

    private String getChildSiddhiAppName(WorkerPodInfo workerPodInfo) {
        // TODO extract the Siddhi app name from Worker pod uuid
        String[] splitName = workerPodInfo.getName().split("-");
        return splitName[0] + splitName[1] + splitName[2] + splitName[3];
        // TODO might maintain parent Siddhi app name also. Otherwise impossible to extract siddhi app name from pod
    }

    ////

    public void createChildSiddhiAppDeployments(List<ChildSiddhiAppInfo> childSiddhiAppInfos)
            throws FileNotFoundException {
        for (ChildSiddhiAppInfo childSiddhiAppInfo : childSiddhiAppInfos) {
            createScalableWorkerDeployment(childSiddhiAppInfo.getName(), WORKERS_NAMESPACE); // TODO do
            childSiddhiApps.put(childSiddhiAppInfo.getName(), childSiddhiAppInfo);
        }
        updateDeployments();
    }

    private void createScalableWorkerDeployment(String childSiddhiAppName, String namespace)
            throws FileNotFoundException {
        // TODO be sure about namespaces
        // Create Service
        Service service = kubernetesClient.services()
                .inNamespace(namespace)
                .create(constructWorkerService(childSiddhiAppName));
        System.out.println("Created Service for child Siddhi app: " + childSiddhiAppName); // TODO log

        // Create Deployment
        Deployment deployment = kubernetesClient.apps().deployments()
                .inNamespace(namespace)
                .create(constructWorkerDeployment(childSiddhiAppName));
        System.out.println("Created Deployment for child Siddhi app: " + childSiddhiAppName); // TODO log

        // Create Horizontal Pod Autoscaler // TODO uncomment HPA. On hold for now
//        HorizontalPodAutoscaler horizontalPodAutoscaler = kubernetesClient.autoscaling().horizontalPodAutoscalers()
//                .inNamespace(namespace)
//                .create(
//                        constructHorizontalPodAutoscaler(
//                                constructHorizontalPodAutoscalerName(deploymentName), namespace, deploymentName));
//        System.out.println("Created Horizontal Pod Autoscaler: " + horizontalPodAutoscaler); // TODO log

        // OLD BELOW
        // Create Deployment
//        Deployment deployment =
//                kubernetesClient.apps().deployments().inNamespace(namespace).create(getWorkerDeployment(deploymentName));
//
//        System.out.println("Created deployment: " + deployment); // TODO log
//
//        // Assign the Deployment to a Horizontal Pod Autoscaler
//        HorizontalPodAutoscaler horizontalPodAutoscaler =
//                constructHorizontalPodAutoscaler(constructHorizontalPodAutoscalerName(deploymentName), namespace, deploymentName);
//        HorizontalPodAutoscaler createdHorizontalPodAutoscaler =
//                kubernetesClient.autoscaling().horizontalPodAutoscalers().inNamespace(namespace).create(horizontalPodAutoscaler);
//
//        System.out.println("Created Horizontal Pod Autoscaler on deployment: " + deployment.getMetadata().getName()); // TODO log
    }

    private HasMetadata constructResourceFromFile(String absoluteFilePath) throws FileNotFoundException {
        List<HasMetadata> resources = kubernetesClient.load(new FileInputStream(absoluteFilePath)).get();
        return resources.get(0);
    }

    private Map<String, String> constructLabels(String childSiddhiAppName) {
        Map<String, String> labels = new HashMap<>();
        labels.put(APP_LABEL_KEY, childSiddhiAppName);
        labels.put(NODE_LABEL_KEY, childSiddhiAppName);
        labels.put(PATTERN_LABEL_KEY, PATTERN_LABEL_VALUE);
        return labels;
    }

    private Service constructWorkerService(String childSiddhiAppName) throws FileNotFoundException {
        return new ServiceBuilder()
                .withApiVersion("v1")
                .withKind("Service")

                // metadata [BEGIN]
                .withNewMetadata()
                .withName(childSiddhiAppName)
                .withLabels(constructLabels(childSiddhiAppName))
                .endMetadata()
                // metadata [END]

                // spec [BEGIN]
                .withNewSpec()
                // type
                .withType("LoadBalancer")
                // ports
                .withPorts(constructWorkerServicePorts())
                // selector
                .withSelector(constructLabels(childSiddhiAppName))
                .endSpec()
                // spec [END]

                .build();

//        Service service = (Service) constructResourceFromFile(WORKER_SERVICE_TEMPLATE_ABSOLUTE_FILE_PATH);
//        service.getMetadata().setName(childSiddhiAppName); // TODO Do other modifications too
//        service.getMetadata().setLabels(constructLabels(childSiddhiAppName));
//        service.getSpec().setSelector(constructLabels(childSiddhiAppName));
//        return service;
    }

    private List<ServicePort> constructWorkerServicePorts() {
        List<ServicePort> servicePorts = new ArrayList<>();
        servicePorts.add(
                new ServicePortBuilder()
                        .withName("http-port-1")
                        .withPort(9090)
                        .withProtocol("TCP")
                        .build());
        servicePorts.add(
                new ServicePortBuilder()
                        .withName("https-port-1")
                        .withPort(9443)
                        .withProtocol("TCP")
                        .build());
        servicePorts.add(
                new ServicePortBuilder()
                        .withName("https-port-2")
                        .withPort(9544)
                        .withProtocol("TCP")
                        .build());
        servicePorts.add(
                new ServicePortBuilder()
                        .withName("https-port-3")
                        .withPort(7070)
                        .withProtocol("TCP")
                        .build());
        servicePorts.add(
                new ServicePortBuilder()
                        .withName("https-port-4")
                        .withPort(7443)
                        .withProtocol("TCP")
                        .build());
        return servicePorts;
    }

    private Deployment constructWorkerDeployment(String childSiddhiAppName) throws FileNotFoundException {
        return new DeploymentBuilder()
                .withApiVersion("apps/v1")
                .withKind("Deployment")

                // metadata [BEGIN]
                .withNewMetadata()
                .withName(childSiddhiAppName)
                .withLabels(constructLabels(childSiddhiAppName))
                .endMetadata()
                // metadata [END]

                // spec [BEGIN]
                .withNewSpec()
                // selector
                .withNewSelector()
                .withMatchLabels(constructLabels(childSiddhiAppName))
                .endSelector()

                // strategy
                .withNewStrategy()
                .withType("Recreate")
                .endStrategy()

                // replicas
                .withReplicas(1)

                // template [BEGIN]
                .withNewTemplate()
                // metadata
                .withNewMetadata()
                .withLabels(constructLabels(childSiddhiAppName))
                .endMetadata()
                // spec
                .withNewSpec()
                .withContainers(constructWorkerDeploymentContainer(childSiddhiAppName))
                .endSpec()
                .endTemplate()
                // template [END]

                .endSpec()
                // spec [END]
        .build();


//        Deployment deployment = (Deployment) constructResourceFromFile(WORKER_DEPLOYMENT_TEMPLATE_ABSOLUTE_FILE_PATH);
//        deployment.getMetadata().setName(childSiddhiAppName); // TODO do other modifications too
//        deployment.getMetadata().setLabels(constructLabels(childSiddhiAppName));
//        deployment.getSpec().getSelector().setMatchLabels(constructLabels(childSiddhiAppName));
//        deployment.getSpec().getTemplate().getMetadata().setLabels(constructLabels(childSiddhiAppName));
//        return deployment;
    }

    private Container constructWorkerDeploymentContainer(String childSiddhiAppName) {
        return new ContainerBuilder()
                .withImage("senthuran16/wso2sp-worker:4.3.0")
                .withName(childSiddhiAppName)
                .withImagePullPolicy("Always")
                .withCommand("sh", "-c", "sleep 40 && /home/wso2carbon/init.sh")
                .withEnv(
                        new EnvVarBuilder().withName("WSO2_SERVER_PROFILE").withValue("worker").build(),
                        new EnvVarBuilder().withName("OFFSET").withValue("0").build(),
                        new EnvVarBuilder().withName("RECEIVER_NODE").withValue("false").build(),
                        new EnvVarBuilder().withName("NODE_PORT").withValue("9443").build(),
                        new EnvVarBuilder()
                                .withName("NODE_IP")
                                .withNewValueFrom()
                                .withNewFieldRef()
                                .withFieldPath("status.podIP")
                                .endFieldRef()
                                .endValueFrom()
                                .build())
                .withPorts(
                        new ContainerPortBuilder().withContainerPort(9090).withProtocol("TCP").build(),
                        new ContainerPortBuilder().withContainerPort(9443).withProtocol("TCP").build(),
                        new ContainerPortBuilder().withContainerPort(9543).withProtocol("TCP").build(),
                        new ContainerPortBuilder().withContainerPort(9544).withProtocol("TCP").build(),
                        new ContainerPortBuilder().withContainerPort(9711).withProtocol("TCP").build(),
                        new ContainerPortBuilder().withContainerPort(9611).withProtocol("TCP").build(),
                        new ContainerPortBuilder().withContainerPort(7711).withProtocol("TCP").build(),
                        new ContainerPortBuilder().withContainerPort(7611).withProtocol("TCP").build(),
                        new ContainerPortBuilder().withContainerPort(7070).withProtocol("TCP").build(),
                        new ContainerPortBuilder().withContainerPort(7443).withProtocol("TCP").build())
                .withLivenessProbe(
                        new ProbeBuilder()
                                .withNewTcpSocket()
                                .withNewPort(9090)
                                .endTcpSocket()
                                .withInitialDelaySeconds(300)
                                .withPeriodSeconds(20)
                                .build())
                .build();
    }

    private HorizontalPodAutoscaler constructHorizontalPodAutoscaler(String name,
                                                                     String namespace,
                                                                     String deploymentName) {
        return new HorizontalPodAutoscalerBuilder()
                .withNewMetadata().withName(name).withNamespace(namespace).endMetadata()
                .withNewSpec()
                .withNewScaleTargetRef()
                .withApiVersion("apps/v1")
                .withKind("Deployment")
                .withName(deploymentName)
                .endScaleTargetRef()
                .withMinReplicas(1)
                .withMaxReplicas(10)
                .addToMetrics(new MetricSpecBuilder()
                        .withType("Resource")
                        .withNewResource()
                        .withName("cpu")
                        .withTargetAverageUtilization(50)
                        .endResource()
                        .build())
                .endSpec()
                .build();
    }

    private String constructHorizontalPodAutoscalerName(String deploymentName) {
        return deploymentName + "-hpa"; // TODO confirm
    }

    private void deployChildSiddhiApp(ChildSiddhiAppInfo childSiddhiAppInfo, String podName) {
        // TODO implement
    }

    // TODO remove if file template is going to be used
    private Deployment getWorkerDeployment(String name) {
        return new DeploymentBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .withNewSpec()
//                .withReplicas(1)
                .withNewTemplate()
                .withNewMetadata()
                .addToLabels("app", name)
                .endMetadata()
                .withNewSpec()

                .addNewContainer()
                .withName(name)
                .withImage("wso2/wso2sp-worker")
                .withCommand("sh", "-c", "sleep 40 && /home/wso2carbon/init.sh")
                .addNewPort()
                .withContainerPort(80)
                .endPort()
                .endContainer()

                .endSpec()
                .endTemplate()
                .withNewSelector()
                .addToMatchLabels("app", name)
                .endSelector()
                .endSpec()
                .build();

//        return new DeploymentBuilder()
//                .withNewMetadata()
//                .withName(name)
//                .endMetadata()
//                .withNewSpec()
////                .withReplicas(1)
//                .withNewTemplate()
//                .withNewMetadata()
//                .addToLabels("app", name)
//                .endMetadata()
//                .withNewSpec()
//
//                .addNewContainer()
//                .withName("nginx")
//                .withImage("k8s.gcr.io/hpa-example")
//                .addNewPort()
//                .withContainerPort(80)
//                .endPort()
//                .endContainer()
//
//                .endSpec()
//                .endTemplate()
//                .withNewSelector()
//                .addToMatchLabels("app", name)
//                .endSelector()
//                .endSpec()
//                .build();
    }
}
