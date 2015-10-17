package com.dredgeplatform.dredge.services;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.dredgeplatform.dredge.clustermanagement.ClusterManager;
import com.dredgeplatform.dredge.clustermanagement.WebserverManager;

@Path("/dredge")
public class ClusterService {
    @GET
    @Path("/startCluster/{clusterName}/{nodesCnt}")
    @Produces(MediaType.TEXT_PLAIN)
    public String startCluster(@PathParam("clusterName") String clusterName, @PathParam("nodesCnt") int nodesCnt) throws Exception {
        ClusterManager.startCluster(nodesCnt, clusterName);
        return clusterName + "Started";
    }

    @GET
    @Path("/stopCluster/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String stopCluster(@PathParam("clusterName") String clusterName) {
        try {
            ClusterManager.stopCluster(clusterName);
        } catch (final Exception e) {
            return e.toString();
        }
        return clusterName + "Stopped";
    }

    @GET
    @Path("/startWebserver/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String startWebserver(@PathParam("clusterName") String clusterName, @PathParam("nodesCnt") int nodesCnt) throws Exception {
        WebserverManager.startWebserver(clusterName);
        return clusterName + " - Webserver Started";
    }

    @GET
    @Path("/stopWebserver/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String stopWebserver(@PathParam("clusterName") String clusterName) throws Exception {
        WebserverManager.stopWebserver(clusterName);
        return clusterName + " - Webserver Stopped";
    }

    @GET
    @Path("/getWebserverStatus/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getWebserverStatus(@PathParam("clusterName") String clusterName) {
        try {
            return WebserverManager.getWebServerStatus(clusterName);
        } catch (final Exception e) {
            return clusterName + " - " + e.getMessage();
        }

    }

}
