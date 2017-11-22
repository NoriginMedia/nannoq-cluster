package com.nannoq.tools.cluster;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.JksOptions;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This class defines helpers for operating the cluster. It can produce a member list, sets eventbus SSL, and produces
 * the modified cluster.xml file as a result of port scanning.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class ClusterUtils {
    private static final Logger logger = LoggerFactory.getLogger(ClusterUtils.class.getSimpleName());

    public static void clusterReport(Long aLong) {
        Vertx vertx = Vertx.currentContext().owner();

        if (vertx.isClustered()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Cluster Members:\n");
            sb.append("----------------\n");

            Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
            instances.stream().findFirst().ifPresent(hz ->
                    hz.getCluster().getMembers().stream()
                            .map(member -> member.getSocketAddress().getAddress().toString() + ":" +
                                    member.getSocketAddress().getPort())
                            .forEach(name -> sb.append(name).append("\n")));

            sb.append("----------------");

            logger.info(sb.toString());
        } else {
            logger.error("Vertx is not clustered!");
        }
    }

    public static EventBusOptions setSSLEventBus(String keystoreName, String keyStoreKey, EventBusOptions eventBusOptions) {
        eventBusOptions.setSsl(true)
                .setKeyStoreOptions(new JksOptions().setPath(keystoreName).setPassword(keyStoreKey))
                .setTrustStoreOptions(new JksOptions().setPath(keystoreName).setPassword(keyStoreKey))
                .setClientAuth(ClientAuth.REQUIRED);

        return eventBusOptions;
    }

    public static void createModifiedClusterConfigByPortScanning(String subnetBase, int thirdElementScanRange,
                                                                 String clusterConfigFileName) {
        String contents = readClusterConfig(clusterConfigFileName);

        if (contents == null) throw new IllegalArgumentException("Could not load cluster config!");

        setClusterMembersForSubnet(subnetBase, thirdElementScanRange, contents, true);
    }
    
    public static void createModifiedClusterConfigByPortScanning(String subnetBase, int thirdElementScanRange,
                                                                 String clusterConfigFileName, boolean dev) {
        String contents = readClusterConfig(clusterConfigFileName);

        if (contents == null) throw new IllegalArgumentException("Could not load cluster config!");

        setClusterMembersForSubnet(subnetBase, thirdElementScanRange, contents, dev);
    }

    private static String readClusterConfig(String clusterConfigFileName) {
        String fileContents = null;

        try {
            CodeSource src = ClusterUtils.class.getProtectionDomain().getCodeSource();

            if (src != null) {
                URL jar = src.getLocation();
                ZipInputStream zip = new ZipInputStream(jar.openStream());

                while (true) {
                    ZipEntry e = zip.getNextEntry();

                    if (e == null)
                        break;

                    if (e.getName().equalsIgnoreCase(clusterConfigFileName)) {
                        fileContents = readZipInputStream(zip);

                        break;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return fileContents;
    }

    private static String readZipInputStream(ZipInputStream stream) throws IOException {
        final StringBuilder sb = new StringBuilder();
        List<String> lines = IOUtils.readLines(stream);
        lines.forEach(sb::append);

        return sb.toString();
    }

    private static void setClusterMembersForSubnet(String subnetBase, int thirdElement, String contents, boolean dev) {
        try {
            AtomicInteger scans = new AtomicInteger();
            AtomicInteger scansComplete = new AtomicInteger();
            StringBuilder replacer = new StringBuilder();
            List<String> CLUSTER_MEMBER_LIST = new CopyOnWriteArrayList<>();

            ExecutorService executorService = Executors.newCachedThreadPool();

            System.out.println("Initializing Port Scan!");

            List<Runnable> portScanners = new CopyOnWriteArrayList<>();

            IntStream.rangeClosed(0, thirdElement).parallel().forEach(baseIpInt ->
                    IntStream.rangeClosed(0, 254).parallel().forEach(lastIpInt -> portScanners.add(() -> {
                        System.out.println("Now running scan for: " + baseIpInt + "." + lastIpInt);

                        if (CLUSTER_MEMBER_LIST.size() == 0) {
                            scans.incrementAndGet();

                            try (Socket socket = new Socket()) {
                                socket.connect(
                                        new InetSocketAddress(subnetBase + baseIpInt + "." + lastIpInt, 5701), 2000);

                                if (socket.isConnected()) {
                                    CLUSTER_MEMBER_LIST.add("<member>" + subnetBase + baseIpInt + "." +
                                            lastIpInt + "</member>");

                                    System.out.println("Member detected at " + subnetBase + baseIpInt + "." +
                                            lastIpInt);
                                }
                            } catch (IOException e) {
                                logger.trace("No connection on: " + subnetBase + baseIpInt + "." + lastIpInt);
                            }

                            scansComplete.incrementAndGet();
                        }
                    })));

            portScanners.forEach(executorService::submit);

            executorService.shutdown();

            while (!executorService.isTerminated()) {
                Thread.sleep(2000L);

                System.out.println("Scan completion Status: (" + scansComplete.get() + "/" + scans.get() + ")");

                if (CLUSTER_MEMBER_LIST.size() > 0) {
                    System.out.println("Scan found at least one member, killing all scanners!");

                    executorService.shutdownNow();

                    break;
                }
            }

            String clusterConfig = contents;

            if (CLUSTER_MEMBER_LIST.size() > 0) {
                CLUSTER_MEMBER_LIST.subList(0, CLUSTER_MEMBER_LIST.size() - 1).forEach(member -> replacer.append(member).append("\n"));
                replacer.append(CLUSTER_MEMBER_LIST.get(CLUSTER_MEMBER_LIST.size() - 1));
                String memberOverview = replacer.toString();

                System.out.println("Finalized Port Scan. Members at: " + memberOverview);

                if (dev) {
                    clusterConfig = contents.replace("<interface>" + subnetBase + "0.0-15</interface>", memberOverview);
                } else {
                    clusterConfig = contents.replace("<interface>" + subnetBase + "0.*</interface>", memberOverview);
                }
            } else {
                System.out.println("Skipping memberlist due to it being empty, doing broad sweep...");
            }

            Path dir = Paths.get("/usr/verticles/cluster-modified.xml");
            File file = dir.toFile();
            //noinspection ResultOfMethodCallIgnored
            file.createNewFile();
            //noinspection ResultOfMethodCallIgnored
            file.setWritable(true);

            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file), StandardCharsets.UTF_8))) {
                bw.write(clusterConfig);
                bw.flush();
                bw.close();
            }
        } catch (IOException | InterruptedException e) {
            logger.error("Error in finding other services!", e);
        }
    }
}
