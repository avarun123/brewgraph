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
  
}