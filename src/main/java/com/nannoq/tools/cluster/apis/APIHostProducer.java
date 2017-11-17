package com.nannoq.tools.cluster.apis;

/**
 * This class defines an interface used to define the routing of internal or external hosts on the specific environment
 * you are running. It is expected by the ApiManager.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public interface APIHostProducer {
    String getInternalHost(String name);
    String getExternalHost(String name);
}
