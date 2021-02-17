
package org.jgroups.protocols.kubernetes;

import org.jgroups.*;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.PingHeader;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Promise;

import java.util.*;
import java.util.logging.Logger;


/**
 * Kubernetes based discovery protocol. Uses the Kubernetes master to fetch the
 * IP addresses of all pods that have been created, then pings each pods
 * separately. The ports are defined by bind_port in TP plus port_range.
 * 
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 * @author Sebastian Łaskawiec
 * @author Bela Ban
 * @author Radoslav Husar
 */
public class KUBE_PING extends Discovery {
    protected static final short KUBERNETES_PING_ID = 2017;
    private final String name = "KUBE_PING";
    private static final Logger log = Logger.getLogger(KUBE_PING.class.getName());


    /*@Property(description = "Number of additional ports to be probed for membership. A port_range of 0 does not "
            + "probe additional ports. Example: initial_hosts=A[7800] port_range=0 probes A:7800, port_range=1 probes "
            + "A:7800 and A:7801")*/
    protected int port_range = 1;

    /*@Property(description = "Max time (in millis) to wait for a connection to the Kubernetes server. If exceeded, "
            + "an exception will be thrown", systemProperty = "KUBERNETES_CONNECT_TIMEOUT")*/
    protected int connectTimeout = 5000;

    /*@Property(description = "Max time (in millis) to wait for a response from the Kubernetes server", systemProperty = "KUBERNETES_READ_TIMEOUT")*/
    protected int readTimeout = 30000;

    /*@Property(description = "Max number of attempts to send discovery requests", systemProperty = "KUBERNETES_OPERATION_ATTEMPTS")*/
    protected int operationAttempts = 3;

    /*@Property(description = "Time (in millis) between operation attempts", systemProperty = "KUBERNETES_OPERATION_SLEEP")*/
    protected long operationSleep = 1000;

    /*@Property(description = "https (default) or http. Used to send the initial discovery request to the Kubernetes server", systemProperty = "KUBERNETES_MASTER_PROTOCOL")*/
    protected String masterProtocol = "https";

    /*@Property(description = "The URL of the Kubernetes server", systemProperty = "KUBERNETES_SERVICE_HOST")*/
    protected String masterHost;

    /*@Property(description = "The port on which the Kubernetes server is listening", systemProperty = "KUBERNETES_SERVICE_PORT")*/
    protected int masterPort;

    /*@Property(description = "The version of the protocol to the Kubernetes server", systemProperty = "KUBERNETES_API_VERSION")*/
    protected String apiVersion = "v1";

    /*@Property(description = "namespace", systemProperty = { "KUBERNETES_NAMESPACE", "OPENSHIFT_KUBE_PING_NAMESPACE" })*/
    protected String namespace = "default";

    /*@Property(description = "The labels to use in the discovery request to the Kubernetes server", systemProperty = {
            "KUBERNETES_LABELS", "OPENSHIFT_KUBE_PING_LABELS" })*/
    protected String labels;

    /*@Property(description = "Certificate to access the Kubernetes server", systemProperty = "KUBERNETES_CLIENT_CERTIFICATE_FILE")*/
    protected String clientCertFile;

    /*@Property(description = "Client key file (store)", systemProperty = "KUBERNETES_CLIENT_KEY_FILE")*/
    protected String clientKeyFile;

    /*@Property(description = "The password to access the client key store", systemProperty = "KUBERNETES_CLIENT_KEY_PASSWORD")*/
    protected String clientKeyPassword;

    /*@Property(description = "The algorithm used by the client", systemProperty = "KUBERNETES_CLIENT_KEY_ALGO")*/
    protected String clientKeyAlgo = "RSA";

    /*@Property(description = "Location of certificate bundle used to verify the serving certificate of the apiserver. If the specified file is unavailable, "
            + "a warning message is issued.", systemProperty = "KUBERNETES_CA_CERTIFICATE_FILE")*/
    protected String caCertFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";

    /*@Property(description = "Token file", systemProperty = "SA_TOKEN_FILE")*/
    protected String saTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token";

    /*@Property(description = "Dumps all discovery requests and responses to the Kubernetes server to stdout when true")*/
    protected boolean dump_requests;

    /*@Property(description = "The standard behavior during Rolling Update is to put all Pods in the same cluster. In"
            + " cases (application level incompatibility) this causes problems. One might decide to split clusters to"
            + " 'old' and 'new' during that process", systemProperty = "KUBERNETES_SPLIT_CLUSTERS_DURING_ROLLING_UPDATE")*/
    protected boolean split_clusters_during_rolling_update;

    /*@Property(description = "Introduces similar behaviour to Kubernetes Services (using DNS) with publishNotReadyAddresses set to true. "
            + "By default it's true", systemProperty = "KUBERNETES_USE_NOT_READY_ADDRESSES")*/
    protected boolean useNotReadyAddresses = true;

    protected Client client;

    protected int tp_bind_port;

    private Address localAddress;

    public boolean isDynamic() {
        return false; // bind_port in the transport needs to be fixed (cannot be 0)
    }

    public void setMasterHost(String masterMost) {
        this.masterHost = masterMost;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    protected boolean isClusteringEnabled() {
        return namespace != null;
    }

    public void init() throws Exception {
        super.init();
        tp_bind_port = getTransport().getBindPort();
        if (tp_bind_port <= 0)
            throw new IllegalArgumentException(String.format("%s only works with  %s.bind_port > 0",
                    KUBE_PING.class.getSimpleName(), getTransport().getClass().getSimpleName()));

        checkDeprecatedProperties();

        if (namespace == null) {
            log.warning("namespace not set; clustering disabled");
            return; // no further initialization necessary
        }
        log.info(String.format("namespace %s set; clustering enabled", namespace));
        
        
        client = new Client();
        log.info("KubePING configuration: " + toString());
    }

    private void checkDeprecatedProperties() {
        checkDeprecatedProperty("KUBERNETES_NAMESPACE", "OPENSHIFT_KUBE_PING_NAMESPACE");
        checkDeprecatedProperty("KUBERNETES_LABELS", "OPENSHIFT_KUBE_PING_LABELS");
    }

    private void checkDeprecatedProperty(String property_name, String deprecated_name) {
        boolean propertyDefined = isPropertyDefined(property_name);
        boolean deprecatedDefined = isPropertyDefined(deprecated_name);
        if (propertyDefined && deprecatedDefined)
            log.warning(String.format("Both %s and %s are defined, %s is deprecated so please remove it", property_name, deprecated_name,
            deprecated_name));
        else if (deprecatedDefined)
            log.warning(String.format("%s is deprecated, please remove it and use %s instead", deprecated_name, property_name));
    }

    private static boolean isPropertyDefined(String property_name) {
        return System.getProperty(property_name) != null || System.getenv(property_name) != null;
    }


    /*@ManagedOperation(description = "Asks Kubernetes for the IP addresses of all pods")*/
    public String fetchFromKube() {
        List<Pod> list = readAll();
        return list.toString();
    }

    protected List<Pod> readAll() {
        if (isClusteringEnabled() && client != null) {
            try {
                return client.getPods(namespace, labels, dump_requests);
            } catch (Exception e) {
                log.warning(String.format("failed getting JSON response from Kubernetes namespace [%s], labels [%s]; encountered [%s: %s]",
                namespace, labels, e.getClass().getName(), e.getMessage()));
            }
        }
        return Collections.emptyList();
    }

    protected void sendDiscoveryRequest(Message req) {
        try {
            down_prot.down(new Event(Event.MSG, req));
        } catch (Throwable t) {
            log.warning(String.format("sending discovery request to %s failed: %s", req.getDest(), t));
        }
    }

    @Override
    public String toString() {
        return String.format("KubePing{namespace='%s', labels='%s'}", namespace, labels);
    }

    @Override
    public void sendGetMembersRequest(Promise promise) throws Exception {
        List<Pod> pods = readAll();
        List<Address> clusterMembers = new ArrayList<>();

        if (pods != null) {
            for (Pod pod : pods) {
                if (!pod.isReady())// && !useNotReadyAddresses)
                    continue;
                for (int i = 0; i <= port_range; i++) {
                    try {
                        IpAddress addr = new IpAddress(pod.getPodIp(), tp_bind_port + i);
                        if (!clusterMembers.contains(addr))
                        clusterMembers.add(addr);
                    } catch (Exception ex) {
                        log.warning(String.format("failed translating host %s into InetAddress: %s", pod, ex));
                    }
                }
            }
        }

        log.info(String.format("%s: sending discovery requests to %s", localAddress, clusterMembers));
        
        for (final Address addr : clusterMembers) {
            if (addr.equals(localAddress)) // no need to send the request to myself
                continue;

            final Message msg = new Message(addr, null, null);
            msg.setFlag(Message.OOB);
            msg.putHeader(name, new PingHeader(PingHeader.GET_MBRS_REQ, null));

            down_prot.down(new Event(Event.MSG, msg));

            /*getTransport().getTimer().execute(new Runnable() {
                public void run() {
                    try{
                        down_prot.down(new Event(Event.MSG, msg));
                    }catch(Exception ex){
                            log.warning(String.format("failed sending discovery request to " + addr, ex));
                    }
                }
            });*/
            try{
                down_prot.down(new Event(Event.MSG, msg));
            }catch(Exception ex){
                    log.warning(String.format("failed sending discovery request to " + addr, ex));
            }
        }

    }

    @Override
    public String getName() {
        return this.name;
    }

    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("namespace");              // max time to wait for initial members
        if(str != null) {
            namespace=str;
            props.remove("namespace");
        }

        str=props.getProperty("labels");  // wait for at most n members
        if(str != null) {
            labels=str;
            props.remove("labels");
        }

        if(!props.isEmpty()) {
            StringBuilder sb=new StringBuilder();
            for(Enumeration<?> e=props.propertyNames(); e.hasMoreElements();) {
                sb.append(e.nextElement().toString());
                if(e.hasMoreElements()) {
                    sb.append(", ");
                }
            }
            
            log.warning(String.format("The following properties are not recognized: " + sb));
            return false;
        }
        return true;
    }

    public void localAddressSet(Address addr) {
        this.localAddress = addr;
        log.info(String.format("Local address set to %s", addr));
    }

}
