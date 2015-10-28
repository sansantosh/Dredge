package com.dredgeplatform.dredge.webserver;

public interface WebserverService {
    void startWebserver() throws Exception;

    void stopWebserver() throws Exception;

    String getWebserverStatus();
}
