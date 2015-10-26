package com.dredgeplatform.dredge.services;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.json.JSONArray;
import org.json.JSONObject;

import com.dredgeplatform.dredge.clustermanagement.ClusterManager;
import com.dredgeplatform.dredge.clustermanagement.WebserverManager;
import com.dredgeplatform.dredge.jobmanagement.SchedulerManager;

@Path("/dredge")
public class ClusterService {
    @GET
    @Path("/startCluster/{clusterName}/{nodesCnt}")
    @Produces(MediaType.TEXT_PLAIN)
    public String startCluster(@PathParam("clusterName") String clusterName, @PathParam("nodesCnt") int nodesCnt) throws Exception {
        try {
            final JSONObject jo = new JSONObject();
            jo.put("status", ClusterManager.startCluster(nodesCnt, clusterName));
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        } catch (final Exception e) {
            final JSONObject jo = new JSONObject();
            jo.put("status", clusterName + " - " + e.getMessage());
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        }
    }

    @GET
    @Path("/stopCluster/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String stopCluster(@PathParam("clusterName") String clusterName) {
        try {
            final JSONObject jo = new JSONObject();
            jo.put("status", ClusterManager.stopCluster(clusterName));
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        } catch (final Exception e) {
            final JSONObject jo = new JSONObject();
            jo.put("status", clusterName + " - " + e.getMessage());
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        }
    }

    @GET
    @Path("/getNodeCnt/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getNodeCnt(@PathParam("clusterName") String clusterName) {
        try {
            final JSONObject jo = new JSONObject();
            jo.put("cnt", ClusterManager.getNodeCnt(clusterName));
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        } catch (final Exception e) {
            final JSONObject jo = new JSONObject();
            jo.put("cnt", clusterName + " - " + e.getMessage());
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        }
    }

    @GET
    @Path("/startWebserver/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String startWebserver(@PathParam("clusterName") String clusterName, @PathParam("nodesCnt") int nodesCnt) throws Exception {
        return WebserverManager.startWebserver(clusterName);
    }

    @GET
    @Path("/stopWebserver/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String stopWebserver(@PathParam("clusterName") String clusterName) {
        try {
            final JSONObject jo = new JSONObject();
            jo.put("status", WebserverManager.stopWebserver(clusterName));
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        } catch (final Exception e) {
            final JSONObject jo = new JSONObject();
            jo.put("status", clusterName + " - " + e.getMessage());
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        }
    }

    @GET
    @Path("/getWebserverStatus/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getWebserverStatus(@PathParam("clusterName") String clusterName) {
        try {
            final JSONObject jo = new JSONObject();
            jo.put("status", WebserverManager.getWebServerStatus(clusterName));
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        } catch (final Exception e) {
            final JSONObject jo = new JSONObject();
            jo.put("status", clusterName + " - " + e.getMessage());
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        }
    }

    @GET
    @Path("/startScheduler/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String startScheduler(@PathParam("clusterName") String clusterName) {
        try {
            final JSONObject jo = new JSONObject();
            jo.put("status", SchedulerManager.startScheduler(clusterName));
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        } catch (final Exception e) {
            final JSONObject jo = new JSONObject();
            jo.put("status", clusterName + " - " + e.getMessage());
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        }
    }

    @GET
    @Path("/stopScheduler/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String stopScheduler(@PathParam("clusterName") String clusterName) {
        try {
            final JSONObject jo = new JSONObject();
            jo.put("status", SchedulerManager.stopScheduler(clusterName));

            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        } catch (final Exception e) {
            final JSONObject jo = new JSONObject();
            jo.put("status", clusterName + " - " + e.getMessage());

            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        }
    }

    @GET
    @Path("/getSchedulerStatus/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getSchedulerStatus(@PathParam("clusterName") String clusterName) {
        try {
            final JSONObject jo = new JSONObject();
            jo.put("status", SchedulerManager.getSchedulerServerStatus(clusterName));
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();

            // final JSONObject mainObj = new JSONObject();
            // mainObj.put("dredge", ja);
            // return mainObj.getJSONArray("dredge").toString();

        } catch (final Exception e) {
            final JSONObject jo = new JSONObject();
            jo.put("status", clusterName + " - " + e.getMessage());
            final JSONArray ja = new JSONArray();
            ja.put(jo);
            return ja.toString();
        }
    }

    @GET
    @Path("/startJob/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String startJob(@PathParam("clusterName") String clusterName) {
        try {
            SchedulerManager.startJob(clusterName);
            return "Job Started";
        } catch (final Exception e) {
            return clusterName + " - " + e.getMessage();
        }
    }

    @GET
    @Path("/stopJob/{clusterName}")
    @Produces(MediaType.TEXT_PLAIN)
    public String stopJob(@PathParam("clusterName") String clusterName) {
        try {
            SchedulerManager.stopJob(clusterName);
            return "Job Stopped";
        } catch (final Exception e) {
            return clusterName + " - " + e.getMessage();
        }
    }
}
