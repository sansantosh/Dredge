package com.dredgeplatform.dredge.webserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dredgeplatform.dredge.clustermanagement.ClusterService;

public class WebserverServiceimpl implements Service, WebserverService {
    private static final long serialVersionUID = 1L;
    final static Logger log = LoggerFactory.getLogger(WebserverServiceimpl.class);

    public String clusterName;
    public int port;
    public static Server jettyServer;

    public WebserverServiceimpl(String clusterName, int port) {
        this.clusterName = clusterName;
        this.port = port;
        log.debug("Cluster Name: {} Port: {}", clusterName, port);
    }

    @Override
    public void init(ServiceContext ctx) {
        log.debug("Webserver Service Initialized. Service Name: {}", ctx.name());
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        log.debug("Webserver Service Execution Started. Service Name: {}", ctx.name());
        startWebserver();
        log.debug("Webserver Service Execution Completed. Service Name: {}", ctx.name());

    }

    @Override
    public void cancel(ServiceContext ctx) {
        try {
            stopWebserver();
        } catch (final Exception e) {
            log.error("ERROR: Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
        }
        log.debug("Webserver Service Cancelled. Service Name: {}", ctx.name());
    }

    @Override
    public void startWebserver() throws Exception {
        jettyServer = new Server(port);

        final ResourceHandler resource_handler1 = new ResourceHandler();
        resource_handler1.setDirectoriesListed(true);
        resource_handler1.setWelcomeFiles(new String[] { "index.html" });
        resource_handler1.setResourceBase("./dredge-webapp");

        final ContextHandler contextWeb = new ContextHandler();
        contextWeb.setContextPath("/dredge");
        contextWeb.setHandler(resource_handler1);

        final ServletContextHandler contextService = new ServletContextHandler(ServletContextHandler.SESSIONS);
        contextService.setContextPath("/");

        final ServletHolder jerseyServlet = contextService.addServlet(org.glassfish.jersey.servlet.ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);
        jerseyServlet.setInitParameter("jersey.config.server.provider.classnames", ClusterService.class.getCanonicalName());

        final ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(new Handler[] { contextService, contextWeb });

        jettyServer.setHandler(contexts);

        new JettyStopThread().start();
        jettyServer.start();
    }

    @Override
    public void stopWebserver() throws UnknownHostException, IOException {
        final Socket s = new Socket(InetAddress.getByName("127.0.0.1"), 8079);
        final OutputStream out = s.getOutputStream();
        log.debug("Sending Jetty Stop Message");
        out.write("\r\n".getBytes());
        out.flush();
        s.close();
    }

    private static class JettyStopThread extends Thread {

        private ServerSocket socket;

        public JettyStopThread() {
            setDaemon(true);
            setName("JettyStopThread");
            try {
                socket = new ServerSocket(8079, 1, InetAddress.getByName("127.0.0.1"));
            } catch (final Exception e) {
                log.error("ERROR: Starting Jetty Stop Thread. Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run() {
            log.debug("Starting Jetty Stop Thread");
            Socket accept;
            try {
                accept = socket.accept();
                final BufferedReader reader = new BufferedReader(new InputStreamReader(accept.getInputStream()));
                reader.readLine();
                log.debug("Stopping Jetty Server");
                jettyServer.stop();
                jettyServer.destroy();
                accept.close();
                socket.close();
            } catch (final Exception e) {
                log.error("ERROR: Stopping Jetty in Stop Thread. Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String getWebserverStatus() {
        String status;
        try {
            status = jettyServer.getState();
        } catch (final Exception e) {
            log.warn("Jetty Server is Stopped...");
            status = "STOPPED";
        }
        return status;
    }

}
