package org.jgroups.protocols.kubernetes;

import mjson.Json;
import org.jgroups.protocols.kubernetes.stream.StreamProvider;
import org.jgroups.util.Util;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.util.Config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.logging.Logger;

import static org.jgroups.protocols.kubernetes.Utils.openStream;
import static org.jgroups.protocols.kubernetes.Utils.urlencode;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class Client {
    protected final String              masterUrl;
    protected final Map<String, String> headers;
    protected final int                 connectTimeout;
    protected final int                 readTimeout;
    protected final int                 operationAttempts;
    protected final long                operationSleep;
    protected final StreamProvider      streamProvider;
    protected final String              info;
    private static final Logger log = Logger.getLogger(Client.class.getName());

    public Client(String masterUrl, Map<String, String> headers, int connectTimeout, int readTimeout, int operationAttempts,
                  long operationSleep, StreamProvider streamProvider) {
        this.masterUrl = masterUrl;
        this.headers = headers;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.operationAttempts = operationAttempts;
        this.operationSleep = operationSleep;
        this.streamProvider = streamProvider;
        Map<String, String> maskedHeaders=new TreeMap<>();
        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                String key = header.getKey();
                String value = header.getValue();
                if ("Authorization".equalsIgnoreCase(key) && value != null)
                    value = "#MASKED:" + value.length() + "#";
                maskedHeaders.put(key, value);
            }
        }
        info=String.format("%s[masterUrl=%s, headers=%s, connectTimeout=%s, readTimeout=%s, operationAttempts=%s, " +
                             "operationSleep=%s, streamProvider=%s]",
                           getClass().getSimpleName(), masterUrl, maskedHeaders, connectTimeout, readTimeout,
                           operationAttempts, operationSleep, streamProvider);
    }

    public String info() {
        return info;
    }

    protected String fetchFromKubernetes(String op, String namespace, String labels, boolean dump_requests) throws Exception {
        String url = masterUrl;
        if(namespace != null && !namespace.isEmpty())
            url = url + "/namespaces/" + urlencode(namespace);
        url = url + "/" + op;
        if(labels != null && !labels.isEmpty())
            url = url + "?labelSelector=" + urlencode(labels);

        InputStream stream=null;
        String retval=null;
        try {
            stream=openStream(url, headers, connectTimeout, readTimeout, operationAttempts, operationSleep, streamProvider);
            
            retval=readContents(stream);//Util.readContents(stream);
            System.out.println("################################### This is retval -> " + retval);
            if(dump_requests)
                System.out.printf("--> %s\n<-- %s\n", url, retval);
            return retval;
        }
        catch(Throwable t) {
            retval=t.getMessage();
            if(dump_requests)
                System.out.printf("--> %s\n<-- ERROR: %s\n", url, t.getMessage());
            throw t;
        }
        finally {
            Util.close(stream);
        }
    }



    private String readContents(InputStream stream) {

        StringBuilder textBuilder = new StringBuilder();
        try {
            Reader reader = new BufferedReader(new InputStreamReader
        (stream, Charset.forName(StandardCharsets.UTF_8.name())));

            int c = 0;
            while ((c = reader.read()) != -1) {
                textBuilder.append((char) c);
            }
        }catch(IOException ioe){
            log.warning(ioe.getStackTrace().toString());
        }

        return textBuilder.toString();
    }

    public List<Pod> getPods(String namespace, String labels, boolean dump_requests) throws Exception {
        //get all pods

        ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);

        CoreV1Api api = new CoreV1Api();
        V1PodList podList = api.listPodForAllNamespaces(null, null, null, labels, null, null, null, null, null, null);
        
        //String result = fetchFromKubernetes("pods", namespace, labels, dump_requests);
        if(podList == null || podList.getItems().size() == 0)
            return Collections.emptyList();

        return parsePodList(podList, namespace, labels);
    }

    /**
     * get pod group during Rolling Update
     * @param pod - json returned by k8s
     * @return
     */
    //TODO:
    String getPodGroup(Json pod) {
        Json meta = Optional.ofNullable(pod.at("metadata")).orElse(null);
        Json labels = Optional.ofNullable(meta)
                .map(podMetadata -> podMetadata.at("labels"))
                .orElse(null);

        // This works for Deployment Config
        String group = Optional.ofNullable(labels)
                .map(l -> l.at("pod-template-hash"))
                .map(Json::asString)
                .orElse(null);

        if (group == null) {
            // Ok, maybe, it's a Deployment and has a valid deployment flag?
            group = Optional.ofNullable(labels)
                    .map(l -> l.at("deployment"))
                    .map(Json::asString)
                    .orElse(null);
        }

        if (group == null) {
            // Final check, maybe it's a StatefulSet?
            group = Optional.ofNullable(labels)
                    .map(l -> l.at("controller-revision-hash"))
                    .map(Json::asString)
                    .orElse(null);
        }

        log.info(String.format("pod %s, group %s", Optional.ofNullable(meta)
        .map(m -> m.at("name"))
        .map(Json::asString)
        .orElse(null), group));
        return group;
    }

    protected List<Pod> parsePodList(V1PodList podList, String namespace, String labels) {
        
        List<Pod> pods=new ArrayList<>();

        for (V1Pod item : podList.getItems()) {
            V1ObjectMeta metadata = item.getMetadata();
            if(metadata.getNamespace().equals(namespace)){
                V1PodStatus podStatus = item.getStatus();
                boolean running = true;//TODO: podRunning(podStatus);
                String parentDeployment = "deployment";//TODO: getPodGroup(obj);
                pods.add(new Pod(metadata.getName(), podStatus.getPodIP(), podStatus.getHostIP(), parentDeployment, running));
                //for(Map.Entry<String, String> entry : item.getMetadata().getLabels().entrySet()){
                   // if(entry.getKey().equals("nscale.component.name") && entry.getValue().equals("application-layer")){
                        //System.out.println(item.getMetadata().getName());
                        //System.out.println(item.getStatus().getHostIP());
                    //}
                //}
            }
                //System.out.println(item.getMetadata().getLabels());
        }


        /*for(Json obj: items) {
            String parentDeployment = getPodGroup(obj);
            String name = Optional.ofNullable(obj.at("metadata"))
                  .map(podMetadata -> podMetadata.at("name"))
                  .map(Json::asString)
                  .orElse(null);
            Json podStatus = Optional.ofNullable(obj.at("status")).orElse(null);
            String podIP = null;
            if(podStatus != null) {
                podIP = Optional.ofNullable(podStatus.at("podIP"))
                  .map(Json::asString)
                  .orElse(null);
            }
            boolean running = podRunning(podStatus);
            if(podIP == null) {
                log.info(String.format("Skipping pod %s since it's IP is %s", name, podIP));
            } else {
                pods.add(new Pod(name, podIP, parentDeployment, running));
            }
        }*/
        log.info(String.format("getPods(%s, %s) = %s", namespace, labels, pods));
        return pods;
    }
    
    /**
     * Helper method to determine if a pod is considered running or not.
     * 
     * @param podStatus a Json object expected to contain the "status" object of a pod
     * @return true if the pod is considered available, false otherwise
     */
    protected boolean podRunning(Json podStatus) {
        if(podStatus == null) {
            return false;
        }
        
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
        log.info("Determining pod status");
        String phase = Optional.ofNullable(podStatus.at("phase"))
                .map(Json::asString)
                .orElse("not running");
        log.info(String.format("  status.phase=%s", phase));
        if(!phase.equalsIgnoreCase("Running")) {
            return false;
        }
        // 2. and 3. status.message and status.reason
        String statusMessage = Optional.ofNullable(podStatus.at("message"))
                .map(Json::asString)
                .orElse(null);
        String statusReason = Optional.ofNullable(podStatus.at("reason"))
                .map(Json::asString)
                .orElse(null);
        log.info(String.format("  status.message=%s and status.reason=%s", statusMessage, statusReason));
        if(statusMessage != null || statusReason != null) {
            return false;
        }
        // 4. status.containerStatuses.ready
        List<Json> containerStatuses = Optional.ofNullable(podStatus.at("containerStatuses"))
                .map(Json::asJsonList)
                .orElse(Collections.emptyList());
        boolean ready = true;
        // if we have no containerStatuses, we don't check for it and consider this condition as passed
        for(Json containerStatus: containerStatuses) {
            ready = ready && containerStatus.at("ready").asBoolean();
        }
        log.info(String.format("  containerStatuses[].status of all container is %s", Boolean.toString(ready)));
        if(!ready) {
            return false;
        }
        // 5. ready condition must be "True"
        Boolean readyCondition = Boolean.FALSE;
        List<Json> conditions = podStatus.at("conditions").asJsonList();
        // walk through all the conditions and find type=="Ready" and get the value of the status property
        for(Json condition: conditions) {
            String type = condition.at("type").asString();
            if(type.equalsIgnoreCase("Ready")) {
                readyCondition = Boolean.parseBoolean(condition.at("status").asString());;//new Boolean(condition.at("status").asString());
            }
        }
        log.info(String.format("conditions with type==\"Ready\" has status property value = %s", readyCondition.toString()));
        if(!readyCondition.booleanValue()) {
            return false;
        }
        return true;
    }
}
