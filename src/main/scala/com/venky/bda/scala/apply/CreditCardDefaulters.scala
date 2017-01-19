package com.venky.bda.scala.apply

object CreditCardDefaulters extends App{
	
  	import com.venky.bda.scala.common._
	import org.apache.spark.sql.functions._
	import org.apache.log4j.Logger
	import org.apache.log4j.Level;

	import org.apache.spark.ml.linalg.{Vector, Vectors}
	import org.apache.spark.ml.feature.LabeledPoint
	import org.apache.spark.sql.Row;
	import org.apache.spark.sql.types._

	import org.apache.spark.ml.classification.DecisionTreeClassifier
	import org.apache.spark.ml.classification.RandomForestClassifier
	import org.apache.spark.ml.classification.NaiveBayes
	import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
	import org.apache.spark.ml.clustering.KMeans
	
	Logger.getLogger("org").setLevel(Level.ERROR);
	Logger.getLogger("akka").setLevel(Level.ERROR);
		
  	val spSession = SparkCommonUtils.spSession
  	val spContext = SparkCommonUtils.spContext
  	val datadir = SparkCommonUtils.datadir
  	
  	//load the CSV file into a RDD
  	println("loading data...")
	val ccRDD1 = spContext.textFile(datadir + "credit-card-default.csv")
	ccRDD1.cache()
	ccRDD1.take(5)
	println("loaed lines : " + ccRDD1.count())
	
	//remove the first line header and invalid lines like aaaaa
	val ccRDD2 = ccRDD1.filter(x =>  
		!( x.startsWith("CUSTID") || 
				x.startsWith("aaaaa")))

	
	//creating schema to load into dataset
	val schema =
	  StructType(
	      StructField("CustId", DoubleType, false) ::
	    StructField("LimitBal", DoubleType, false) ::
	     StructField("Sex", DoubleType, false) ::
	    StructField("Education", DoubleType, false) ::
	    StructField("Marriage", DoubleType, false) ::
	     StructField("Age", DoubleType, false) ::
	    StructField("AvgPayDur", DoubleType, false) ::
	     StructField("AvgBillAmt", DoubleType, false) ::
	   StructField("AvgPayAmt", DoubleType, false) ::
	    StructField("PerPaid", DoubleType, false) ::
	     StructField("Defaulted", DoubleType, false) :: Nil)

	def transformToNumeric( inputStr : String) : Row = {
		
	    val attList=inputStr.split(",")
	    
	    //rounding of ages to immediate tens
		val age:Double = Math.round(attList(5).toDouble /10.0 ) * 10.0;
	    
	    //normalizing sex to 1 & 2
	    val sex:Double = attList(2) match {
	                    case  "M" => 1.0
	                    case  "F" => 0.0
	                    case  _   => attList(2).toDouble
	                }
	    
	    //finding average billed amount
	    val avgBillAmt:Double = Math.abs(
	    					( attList(12).toDouble +
	    						attList(13).toDouble + attList(14).toDouble + attList(15).toDouble + attList(16).toDouble +
	    						attList(17).toDouble ) / 6.0 )
	    								
	    //finding average pay amount
	    val avgPayAmt:Double = Math.abs( 
	    					( attList(18).toDouble + attList(19).toDouble + attList(20).toDouble + attList(21).toDouble +
	    						attList(22).toDouble +
	    						attList(23).toDouble ) / 6.0 )
	    								
	    //finding average pay duration
	    val avgPayDuration:Double = Math.abs( 
	    					( attList(6).toDouble + attList(7).toDouble +
	    						attList(8).toDouble +
	    						attList(9).toDouble +
	    						attList(10).toDouble +
	    						attList(11).toDouble ) / 6.0 )
	    								
	   	//finding percentage paid                   
		var perPay:Double = Math.round((avgPayAmt/(avgBillAmt+1) * 100) / 25.0) * 25.0; 
	    if ( perPay > 100) perPay=100
	    
	    //removing not needed columns
	    val values= Row(attList(0).toDouble, 
	                     attList(1).toDouble,  
	                     sex,
	                     attList(3).toDouble,  
	                     attList(4).toDouble,  
	                     age,
	                     avgPayDuration,
	                     avgBillAmt,
	                     avgPayAmt,
	                     perPay,
	                     attList(24).toDouble 
						)
	    return values
	 }   
	

	val ccVectors = ccRDD2.map(transformToNumeric)
	ccVectors.collect()
    val ccDf1 = spSession.createDataFrame(ccVectors, schema)
    ccDf1.printSchema()
    ccDf1.show(5)

   
    //creating dataframe for gender
	val genderList = Array("{'sexName': 'Male', 'sexId': '1.0'}",
	     				"{ 'sexName':'Female','sexId':'2.0' }")
	val genderDf =  spSession.read.json(spContext.parallelize(genderList))
	val ccDf2 = ccDf1.join(genderDf, ccDf1("Sex") === genderDf("sexId"))
						.drop("sexId").repartition(2)
	
	
	//creating dataframe for education
	val eduList = Array("{'eduName': 'Graduate', 'eduId': '1.0'}",
						"{'eduName': 'University', 'eduId': '2.0'}",
						"{'eduName': 'High School', 'eduId': '3.0'}",
	     				"{'eduName': 'Others', 'eduId': '4.0'}")
	val eduDf =  spSession.read.json(spContext.parallelize(eduList))					
	val ccDf3 = ccDf2.join(eduDf, ccDf1("Education") === eduDf("eduId"))
						.drop("eduId").repartition(2)
						
	
	//creating dataframe for marriage
	val marriageList = Array("{'marriageName': 'Single', 'marriageId': '1.0'}",
						"{'marriageName': 'Married', 'marriageId': '2.0'}",
	     				"{'marriageName': 'Others', 'marriageId': '3.0'}")
	val marriageDf =  spSession.read.json(spContext.parallelize(marriageList))					
	val ccDf4 = ccDf3.join(marriageDf, ccDf1("Marriage") === marriageDf("marriageId"))
						.drop("marriageId").repartition(2)
	
	println("Final Data frame :")
	ccDf4.printSchema()
	ccDf4.show(5)
	
	//>> analysis part <<

	ccDf4.createOrReplaceTempView("CCDATA")
	
    //distinction between males and females in defaulting
	val Male_Female_Diff  = spSession.sql(
			"SELECT sexName, count(*) as Total, " + 
				" SUM(Defaulted) as Defaults, " + 
				" ROUND(SUM(Defaulted) * 100 / count(*)) as PerDefault " + 
				" FROM CCDATA GROUP BY sexName" );
	println("output for Male_Female_Diff")
	Male_Female_Diff.show()
	
	//does martial status affect defaulting?
	val Martial_Correlation  = spSession.sql(
			"SELECT marriageName, eduName, count(*) as Total," +
				" SUM(Defaulted) as Defaults, " + 
				" ROUND(SUM(Defaulted) * 100 / count(*)) as PerDefault " + 
				" FROM CCDATA GROUP BY marriageName, eduName " + 
				" ORDER BY 1,2");
	println("output for Martial_Correlation")
	Martial_Correlation.show()
	
	//indication of future default on basis of prev 6 months
	val Future_Prob  = spSession.sql(
			"SELECT AvgPayDur, count(*) as Total, " + 
				" SUM(Defaulted) as Defaults, " + 
				" ROUND(SUM(Defaulted) * 100 / count(*)) as PerDefault " + 
				" FROM CCDATA GROUP BY AvgPayDur ORDER BY 1");
	println("output for Future_Prob")
	Future_Prob.show()
	



	//tranforming to a data frame for ML algos , dropping few columns...
	
	def transformToLabelVectors(inStr : Row ) : LabeledPoint = { 
	    val labelVectors = new LabeledPoint(inStr.getDouble(0) , 
									Vectors.dense(inStr.getDouble(2),
											inStr.getDouble(3),
											inStr.getDouble(4),
											inStr.getDouble(5),
											inStr.getDouble(6),
											inStr.getDouble(7),
											inStr.getDouble(8),
											inStr.getDouble(9)));
	    return labelVectors
	}

	val ccRDD3 = ccDf4.rdd.repartition(2);
	val ccLabelVectors = ccRDD3.map(transformToLabelVectors)
	ccLabelVectors.collect()
	
	val ccDf5 = spSession.createDataFrame(ccLabelVectors, classOf[LabeledPoint] )
	ccDf5.cache()
	
	val ccMap = ccDf4.select("CustId", "Defaulted")
	val ccDf6 = ccDf5.join(ccMap, ccDf5("label") === ccMap("CustId"))
						.drop("label").repartition(2)
						
	println("final data: ")
	ccDf6.show()

	//splitting data for training and testing
	val Array(trainingData, testData) = ccDf6.randomSplit(Array(0.7, 0.3))
	trainingData.count()
	testData.count()


	// >> To predict defaults. comparing multiple algorithms for the best results <<
	
	val evaluator = new MulticlassClassificationEvaluator()
	evaluator.setPredictionCol("Prediction")
	evaluator.setLabelCol("Defaulted")
	evaluator.setMetricName("accuracy")
	
	// >> Decision Trees Classifier <<
	val dtClassifier = new DecisionTreeClassifier()
	dtClassifier.setLabelCol("Defaulted")
	dtClassifier.setPredictionCol("Prediction")
	dtClassifier.setMaxDepth(4)
	val dtModel = dtClassifier.fit(trainingData)
	val dtPredictions = dtModel.transform(testData)
	println("\nDecision Trees Accuracy = " + evaluator.evaluate(dtPredictions))
	
	// >> Random Forests <<
	val rfClassifier = new RandomForestClassifier()
	rfClassifier.setLabelCol("Defaulted")
	rfClassifier.setPredictionCol("Prediction")
	val rfModel = rfClassifier.fit(trainingData)
	val rfPredictions = rfModel.transform(testData)
	println("\nRandom Forests Accuracy = " + evaluator.evaluate(rfPredictions))
	
	// >> Naive Bayes <<
	val nbClassifier = new NaiveBayes()
	nbClassifier.setLabelCol("Defaulted")
	nbClassifier.setPredictionCol("Prediction")
	val nbModel = nbClassifier.fit(trainingData)
	val nbPredictions = nbModel.transform(testData)
	println("\nNaive Bayes Accuracy = " + evaluator.evaluate(nbPredictions))
	

	//Clustering now - data into 4 groups based on age, education, sex, marriage
	
	val ccDf7 = ccDf4.select("Sex","Education","Marriage","Age","CustId")
	ccDf7.show()
	
	//centering and scaling
	val meanVal = ccDf7.agg(avg("Sex"), avg("Education"),avg("Marriage"),
    		avg("Age")).collectAsList().get(0)
    		
    val stdVal = ccDf7.agg(stddev("Sex"), stddev("Education"),
    		stddev("Marriage"),stddev("Age")).collectAsList().get(0)
    		
    val bcMeans=spContext.broadcast(meanVal)
	val bcStdDev=spContext.broadcast(stdVal)
	
	def centerAndScale(inRow : Row ) : LabeledPoint  = {
	    val meanArray=bcMeans.value
	    val stdArray=bcStdDev.value
	    
	    var retArray=Array[Double]()
	    
	    for (i <- 0 to inRow.size - 2)  {
	    	val csVal = ( inRow.getDouble(i) - meanArray.getDouble(i)) /
	    					 stdArray.getDouble(i)
	        retArray = retArray :+ csVal
	    }

	    return  new LabeledPoint(inRow.getDouble(4),Vectors.dense(retArray))
	} 
	
    val ccRDD4 = ccDf7.rdd.repartition(2);
	val ccRDD5 = ccRDD4.map(centerAndScale)
	ccRDD5.collect()
	 
	val ccDf8 = spSession.createDataFrame(ccRDD5, classOf[LabeledPoint] )

	ccDf8.select("label","features").show(10)
	

	// using k Means method
	val kmeans = new KMeans()
	kmeans.setK(4)
	kmeans.setSeed(1L)
	
	val model = kmeans.fit(ccDf8)
	val predictions = model.transform(ccDf8)

	println("Groupings :")
	predictions.groupBy("prediction").count().show()
	println("Customer Assignments :")
	predictions.select("label","prediction").show()

	spContext.stop()
}
