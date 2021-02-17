package org.jgroups.protocols.kubernetes;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodCondition;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.util.Config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class Client {
    
    private static final Logger log = Logger.getLogger(Client.class.getName());

    public Client() {

    }

    public List<Pod> getPods(String namespace, String labels, boolean dump_requests) throws Exception {
        ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);

        CoreV1Api api = new CoreV1Api();
        V1PodList podList = api.listPodForAllNamespaces(null, null, null, labels, null, null, null, null, null, null);
        
        if(podList == null || podList.getItems().size() == 0)
            return Collections.emptyList();

        return parsePodList(podList, namespace, labels);
    }

    
    String getPodGroup(V1ObjectMeta podMetadata) {
        //looks for Deployment or StatefulSet
        List<String> groupsToLookFor = List.of("pod-template-hash", "deployment", "controller-revision-hash");

        for(Map.Entry<String, String> entry : podMetadata.getLabels().entrySet()){
                    if(groupsToLookFor.contains(entry.getKey()))
                        return entry.getValue();
        }

        return null;
    }

    protected List<Pod> parsePodList(V1PodList podList, String namespace, String labels) {
        List<Pod> pods=new ArrayList<>();

        for (V1Pod item : podList.getItems()) {
            V1ObjectMeta metadata = item.getMetadata();
            if(metadata.getNamespace().equals(namespace)){
                V1PodStatus podStatus = item.getStatus();
                boolean running = podRunning(podStatus);
                String parentDeployment = getPodGroup(metadata);
                pods.add(new Pod(metadata.getName(), podStatus.getPodIP(), parentDeployment, running));
            }
        }

        log.info(String.format("getPods(%s, %s) = %s", namespace, labels, pods));

        return pods;
    }
    
    
    protected boolean podRunning(V1PodStatus podStatus) {
        /*
         * A pod can only be considered 'running' if the following conditions are all true:
         * 1. status.phase == "Running",
         * 2. status.message is Undefined (does not exist)
         * 3. status.reason is Undefined (does not exist)
         * 4. all of status.containerStatuses[*].ready == true
         * 5. for conditions[*].type == "Ready" conditions[*].status must be "True" 
         */
        // walk through each condition step by step
        // 1 status.phase
        if(!podStatus.getPhase().equalsIgnoreCase("Running")) {
            return false;
        }

        // 2. and 3. status.message and status.reason
        if(podStatus.getMessage() != null || podStatus.getReason() != null) {
            return false;
        }

        // 4. status.containerStatuses.ready
        List<V1ContainerStatus> containerStatuses = podStatus.getContainerStatuses();
        boolean ready = true;
        // if we have no containerStatuses, we don't check for it and consider this condition as passed
        for(V1ContainerStatus containerStatus: containerStatuses) {
            ready = ready && containerStatus.getReady();
        }
        
        if(!ready) {
            return false;
        }
        // 5. ready condition must be "True"
        Boolean readyCondition = Boolean.FALSE;
        List<V1PodCondition> conditions = podStatus.getConditions();
        // walk through all the conditions and find type=="Ready" and get the value of the status property
        for(V1PodCondition condition: conditions) {
            String type = condition.getType();
            if(type.equalsIgnoreCase("Ready")) {
                readyCondition = Boolean.parseBoolean(condition.getStatus());;//new Boolean(condition.at("status").asString());
            }
        }

        if(!readyCondition.booleanValue()) {
            return false;
        }

        return true;
    }
}
