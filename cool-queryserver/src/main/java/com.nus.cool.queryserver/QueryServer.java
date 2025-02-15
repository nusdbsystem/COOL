package com.nus.cool.queryserver;

import org.eclipse.jetty.server.*;
import org.glassfish.jersey.jetty.JettyHttpContainer;
import org.glassfish.jersey.server.ContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.jackson.JacksonFeature;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jetty9.InstrumentedHandler;
import com.codahale.metrics.jetty9.InstrumentedQueuedThreadPool;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueryServer implements Runnable {

    private Thread thisThread;

    private AtomicBoolean started = new AtomicBoolean(false);

    private CountDownLatch waitForLatch = new CountDownLatch(1);

    private volatile boolean bStop = false;

    private QueryServerModel model;

    private String datasetPath;

    public QueryServer(String path){
        this.datasetPath = path;
        File root = new File(this.datasetPath);
        if(!root.exists()){
            root.mkdir();
            System.out.println("[*] Dataset source folder " + path + " is created.");
        } else {
            System.out.println("[*] Dataset source folder " + path + " exists.");
        }
    }

    @Override
    public void run() {
        System.out.println("[*] Start the Query Server...");
        try {
            this.model = new QueryServerModel(this.datasetPath);
            Server httpServer = createJettyServer(8080, 100, new QueryServerController(this.model));
            httpServer.start();
            httpServer.join();
//            while (!httpServer.isRunning())
//                Thread.sleep(100);
//            waitForLatch.countDown();
//
//            // mainLoop
//            while (!bStop)
//                Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start() {
        if (!started.getAndSet(true)) {
            thisThread = new Thread(this);
            thisThread.start();
        }
    }

    public void waitForStart() throws InterruptedException {
        waitForLatch.await();
    }

    public void join() throws InterruptedException {
        if (started.get()) {
            thisThread.join();
        }
    }

    private Server createJettyServer(int port, int poolSize, Object controller) {
        MetricRegistry jettyMetrics = new MetricRegistry();
        InstrumentedQueuedThreadPool pool = new InstrumentedQueuedThreadPool(
                jettyMetrics, poolSize);

        Server server = new Server(pool);
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        ResourceConfig restConfig = new ResourceConfig();
        // restConfig.registerClasses(QueryServerController.class);
        restConfig.registerInstances(controller);
        restConfig.register(JacksonFeature.class);

        Handler handler = ContainerFactory.createContainer(
                JettyHttpContainer.class, restConfig);
        InstrumentedHandler metricsHandler = new InstrumentedHandler(jettyMetrics);
        metricsHandler.setHandler(handler);

        server.setConnectors(new Connector[]{connector});
        server.setHandler(metricsHandler);
        return server;
    }

    public static void main(String[] args) throws Exception {
        QueryServer qserver = new QueryServer(args[0]);
        qserver.start();
        qserver.waitForStart();
        qserver.join();
    }
}