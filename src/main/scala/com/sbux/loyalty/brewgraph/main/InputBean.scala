package com.sbux.loyalty.brewgraph.main

import java.util.Date
import java.util.Calendar

class InputBean extends java.io.Serializable  {
   var indexOfCountry = 0
  var indexOfRegion = 1
  var indexOfTimeOfDay = 5
  var indexOfItem = 3
  var indexOfUser = 2
  var inputFileName = "/projects/mop/mop-train-pos-basket-short.csv"
  var outFileName = "/projects/sim/output-" + Calendar.getInstance().getTime() + ".txt"
  var alpha:Double =0.0
  var indexOfTxDay = 6
  var indexOfItemCount = 7
  
  var delimiter = ","
  var endDay:Date = null
  var timeDecay:Boolean = false
  var local=false
  var min_intersection:Integer=100;
     def toTextString():String= {
				var retValue= "InputBean [indexOfCountry=" + indexOfCountry + ", indexOfRegion=" + indexOfRegion+ ", indexOfTimeOfDay=" + indexOfTimeOfDay + ", indexOfItem=" + indexOfItem + ", indexOfUser="
						retValue+= indexOfUser + ", inputFileName=" + inputFileName + ", outFileName=" + outFileName + ", alpha="
						retValue+= alpha + ", indexOfTxDay=" + indexOfTxDay + ", indexOfItemCount=" + indexOfItemCount+ ", delimiter=" + delimiter + ", endDay=" + endDay 
						retValue+="timeDecay="+timeDecay+ "]"
						return retValue
			}
}