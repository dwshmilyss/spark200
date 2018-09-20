/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.scheduler.TaskLocality
import org.apache.spark.sql.SparkSession

import scala.math.random

object LocalPi {
  def main(args: Array[String]) {
    var count = 0
    for (i <- 1 to 100000) {
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) count += 1
    }
    println("Pi is roughly " + 4 * count / 100000.0)
    println(TaskLocality.ANY > TaskLocality.RACK_LOCAL)
    println(TaskLocality.RACK_LOCAL > TaskLocality.NO_PREF)
    println(TaskLocality.NO_PREF > TaskLocality.NODE_LOCAL)
    println(TaskLocality.NODE_LOCAL > TaskLocality.PROCESS_LOCAL)


    val spark = SparkSession
      .builder
      .appName("test")
        .master("local[*]")
      .getOrCreate()

    spark.sparkContext.parallelize(1 to 10,2).map(_+1).collect().foreach(println)

  }
}
// scalastyle:on println
