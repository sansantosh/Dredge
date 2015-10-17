package com.dredgeplatform.dredge.clustermanagement;

public interface WebserverService {
    void startWebserver() throws Exception;

    void stopWebserver() throws Exception;

    String getWebserverStatus();
}
