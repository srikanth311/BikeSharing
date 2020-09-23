package com.skkodali.bikesharing.printing

class Display 
{
  def displayArrays(fullArray: Array[Double]) 
  {
    var pos = 0;
    for (m <- fullArray) {
      //println(" postition : " + pos + " and value is : " +m)
      pos = pos + 1
    }
  }
  //
  def displayMap(generalMap: Map[Int, Int]) 
  {
    generalMap.keys.foreach { i =>
      print("Key = " + i)
      println("Value = " + generalMap(i))
    }
  }
}