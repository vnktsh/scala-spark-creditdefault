package com.venky.bda.scala.common

object SparkCommonUtils {
  
	import org.apache.spark.sql.SparkSession
	import org.apache.spark.SparkContext
	import org.apache.spark.SparkConf
	
	val datadir = "/home/venky/academics/scala-spark-creditdefault/data-files/"
	val appName="credit-default"
	val sparkMasterURL = "local[2]"
	val tempDir= "/tmp/spark-tmp"
	
	var spSession:SparkSession = null
	var spContext:SparkContext = null
	 
	{	
		
		//create configuration object
		val conf = new SparkConf()
			.setAppName(appName)
			.setMaster(sparkMasterURL)
			.set("spark.executor.memory","2g")
			.set("spark.sql.shuffle.partitions","2")
	
		//create a spark context	
		spContext = SparkContext.getOrCreate(conf)

		//create a spark SQL session
		spSession = SparkSession
		  .builder()
		  .appName(appName)
		  .master(sparkMasterURL)
		  .config("spark.sql.warehouse.dir", tempDir)  
		  .getOrCreate()

	}
}
