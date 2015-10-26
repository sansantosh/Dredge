function plotSchedulerNodeCnt(dataset) {
	 var circleData = [{ "x_axis": 90, "y_axis": 70, "radius": 50, "color": "green", "cnt" : dataset[0].cnt, "font_size": 22, "x_variance" : 10  }];
	 plotCircle('.schNodeCnt', circleData);
}

function plotComputeNodeCnt(dataset) {
	 var circleData = [{ "x_axis": 90, "y_axis": 70, "radius": 50, "color": "green", "cnt" : dataset[0].cnt, "font_size": 22, "x_variance" : 10 }];
	 plotCircle('.cNodeCnt', circleData);
}	 


function plotWebserverNodeCnt(dataset) {
	 var circleData = [{ "x_axis": 90, "y_axis": 70, "radius": 50, "color": "green", "cnt" : dataset[0].cnt, "font_size": 22, "x_variance" : 10 }];
	 plotCircle('.wsNodeCnt', circleData);
}

function plotchartScheduler(dataset) {
	if (dataset[0].status.toUpperCase() == "STARTED") {
		var color = "green";
	} else {
		var color = "red";
	}
	var circleData = [{ "x_axis": 90, "y_axis": 70, "radius": 50, "color": color, "cnt" : dataset[0].status.toUpperCase(), "font_size": 14, "x_variance" : 35 }];
	 plotCircle('.chart', circleData);
}


function plotchartWebserver(dataset) {
	var circleData = [{ "x_axis": 90, "y_axis": 70, "radius": 50, "color": "green", "cnt" : dataset[0].status.toUpperCase(), "font_size": 14, "x_variance" : 35 }];
    plotCircle('.chart1', circleData);
}
                            
function plotCircle(element, circleData) {
	d3.select("body")
	 	.selectAll("div")
	 	.select(element)
	 	.select("svg").remove();

 	var svgContainer = d3.select("body")
 						.selectAll("div")
 						.select(element)
 						.append("svg");

	var circles = svgContainer.selectAll("circle")
	         .data(circleData)
	         .enter()
	         .append("circle");
	                              
	var circleAttributes = circles
				.attr("cx", function (d) { return d.x_axis; })
				.attr("cy", function (d) { return d.y_axis; })
				.attr("r", function (d) { return d.radius; })
				.style("fill",  function (d) { return d.color; });
	
	var text = svgContainer.selectAll("text")
	          .data(circleData)
	          .enter()
	          .append("text");
	
	var textLabels = text
	   .attr("x", function (d) {  return d.x_axis - d.x_variance; })
	   .attr("y", function (d) {  return d.y_axis + 5; })
	    .text(function (d) {  return d.cnt; })
	   .attr("font-family", "Verdana")
	   .attr("font-size", function (d) {  return d.font_size; }  )
	   .attr("fill", "white");
}

