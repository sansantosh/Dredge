(function(angular) {
    var app = angular.module('dredge-app', ['ngTouch']);
    
    app.controller("ClusterMetrics", function($scope) {
        $scope.cluster = {};
        $scope.cluster.title = "Dredge Cluster Metrics"; 
     });
    
    app.controller("services", function($scope,$http) { 
    	$scope.nodes = '2';
    	$scope.showSchedulerStartbtn = true;
    	$scope.showSchedulerStopbtn = false;
    	
    	$scope.getSchedulerStatus = function() {
    		var url="/dredge/getSchedulerStatus/dredgeScheduler";
    		$http.get(url).success( function(response) { 
    			$scope.status = response;
    			if($scope.status[0].status == "Started") {
    				$scope.showSchedulerStartbtn = false;
        	    	$scope.showSchedulerStopbtn = true;
    			} else {
    				$scope.showSchedulerStartbtn = true;
        	    	$scope.showSchedulerStopbtn = false;
    			}
    			plotchartScheduler($scope.status); 
    		}) 
    	 }
    	
    	$scope.startScheduler = function() {
    		var url="/dredge/startScheduler/dredgeScheduler";
    		$http.get(url).success( function(response) { 
    			$scope.status = response;
    			$scope.showSchedulerStartbtn = false;
    	    	$scope.showSchedulerStopbtn = true;
    	    	plotchartScheduler($scope.status); 
    		}) 
    	 }
    	
    	$scope.stopScheduler = function() {
    		var url="/dredge/stopScheduler/dredgeScheduler";
    		$http.get(url).success( function(response) { 
    			$scope.status = response;
    			$scope.showSchedulerStartbtn = true;
    	    	$scope.showSchedulerStopbtn = false;
    	    	plotchartScheduler($scope.status); 
    		}) 
    	 }
    	
    	
    	$scope.getWebserverStatus = function() {
    		var url="/dredge/getWebserverStatus/dredgeWebserver";
    		$http.get(url).success( function(response) { 
    			$scope.status = response;
    			plotchartWebserver($scope.status); 
    		}) 
    	 }
    	
    	$scope.showWebserverNodes = true;
    	$scope.getWebserverNodesCnt = function() {
    		var url="/dredge/getNodeCnt/dredgeWebserver";
    		$http.get(url).success( function(response) { 
    			$scope.cnt = response;
    			if($scope.cnt[0].cnt.indexOf("Cluster group is empty") > -1) {
    				$scope.cnt[0].cnt = "0";
    				$scope.showWebserverNodes = false;
    			} else {
    				$scope.showWebserverNodes = true;
    			}
    			plotWebserverNodeCnt($scope.cnt); 
    		}) 
    	 }
    	
    	$scope.showComputeNodes = true;
    	$scope.getComputeNodesCnt = function() {
    		var url="/dredge/getNodeCnt/dredgeCompute";
    		$http.get(url).success( function(response) { 
    			$scope.cnt = response;
    			if($scope.cnt[0].cnt.indexOf("Cluster group is empty") > -1) {
    				$scope.cnt[0].cnt = "0";
    				$scope.showComputeNodes = false;
    			} else {
    				$scope.showComputeNodes = true;
    			}
    			plotComputeNodeCnt($scope.cnt); 
    		}) 
    	 }
    	
    	$scope.showSchedulerNodes = true;
    	$scope.getSchedulerNodesCnt = function() {
    		var url="/dredge/getNodeCnt/dredgeScheduler";
    		$http.get(url).success( function(response) { 
    			$scope.cnt = response;
    			if($scope.cnt[0].cnt.indexOf("Cluster group is empty") > -1) {
    				$scope.cnt[0].cnt = "0";
    				$scope.showSchedulerNodes = false;
    			} else {
    				$scope.showSchedulerNodes = true;
    			}
    			plotSchedulerNodeCnt($scope.cnt); 
    		}) 
    	 }
    	
    	$scope.stopCluster = function(ClusterName) {
    		var url="/dredge/stopCluster/" + ClusterName;
    		$http.get(url).success( function(response) { 
    			$scope.cnt = response;
    			if (ClusterName == "dredgeWebserver") {
    				$scope.getWebserverNodesCnt();
    			} else if (ClusterName == "dredgeCompute") {
    				$scope.getComputeNodesCnt();
    			} else if (ClusterName == "dredgeScheduler") {
    				$scope.getSchedulerNodesCnt();
    			}
    		}) 
    	 }
    	
    	$scope.addNodes = function(ClusterName) {
    		var url="/dredge/startCluster/" + ClusterName + "/" + $scope.nodes;
    		$http.get(url).success( function(response) { 
    			$scope.cnt = response;
    			alert(response[0].status);
    			if (ClusterName == "dredgeWebserver") {
    				$scope.getWebserverNodesCnt();
    			} else if (ClusterName == "dredgeCompute") {
    				$scope.getComputeNodesCnt();
    			} else if (ClusterName == "dredgeScheduler") {
    				$scope.getSchedulerNodesCnt();
    			}
    		}) 
    	 }
    	
    	
    	
  });
    
  })(angular);
    