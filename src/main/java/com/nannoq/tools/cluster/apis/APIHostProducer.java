package com.nannoq.tools.cluster.apis;

public interface APIHostProducer {
    String getInternalHost(String name);
    String getExternalHost(String name);
}
