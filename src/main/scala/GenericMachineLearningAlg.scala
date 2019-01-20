import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.types.StringType

import scala.collection.mutable

object GenericMachineLearningAlg {

  def ConvertEx(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkConfDataScienceAPPTest")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("sparkSessionDataScienceAPP")
      .getOrCreate
    spark.conf.set("spark.sql.session.timeZone", "GMT")
    spark.sparkContext.setLogLevel("ERROR")

    // Import dataframe
    val path = args(0)
    val Database = spark.read.format("csv").option("header","true").option("inferschema","true").load(path)

    // Separate Numeric and no Numeric features
    val NoNumFeaturesColNames = mutable.ArrayBuffer[String]()
    val NumFeaturesColNames = mutable.ArrayBuffer[String]()
    Database.schema.map(col => {
      if(col.dataType == StringType){
        NoNumFeaturesColNames += (col.name.toString)
      } else {
        NumFeaturesColNames += (col.name.toString)
      }
    })

    // Label
    val labelColName = args(1) // "Class"
    val idxdLabelColName = labelColName + "Indexed"

    // Features
    val idxdNoNumFeaturesColNames = NoNumFeaturesColNames.map(_ + "Indexed")
    val allFeaturesColNamesInterm = NumFeaturesColNames ++ NoNumFeaturesColNames
    val allIdxdFeaturesColNamesIntermediate = NumFeaturesColNames ++ idxdNoNumFeaturesColNames
    val features = "Features"
    val allFeaturesColNames = allFeaturesColNamesInterm.filter(_ != labelColName)
    val allIdxdFeaturesColNames = allIdxdFeaturesColNamesIntermediate.filter(_ != idxdLabelColName)

    // Train Test split
    val splits = Database.randomSplit(Array(0.7,0.3))
    val (trainingData, testData) = (splits(0),splits(1))
    val allTrainingData = trainingData.select(labelColName, allFeaturesColNames: _*)
    val allTestData = testData.select(labelColName, allFeaturesColNames: _*)
    val allData = allTrainingData.union(allTestData)

    //Encode from string to int Features
    val stringIndexers = NoNumFeaturesColNames.map{colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(allData)
    }

    // Encode from string to int Labels
    val labelIndexer = new StringIndexer()
      .setInputCol(labelColName)
      .setOutputCol(idxdLabelColName)
      .fit(allData)

    // Vector assembler
    val assembler = new VectorAssembler()
      .setInputCols(Array(allIdxdFeaturesColNames: _*))
      .setOutputCol(features)

    // Random Forest
    val randomForest = new RandomForestClassifier()
      .setLabelCol(idxdLabelColName)
      .setFeaturesCol(features)

    // Label Converter 
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictionLabel")
      .setLabels(labelIndexer.labels)

    // Pipeline
    val pipeline = new Pipeline().setStages(
      (stringIndexers  :+ assembler :+ randomForest :+ labelConverter).toArray)

    // Cross validation parameters
    val paramGrid = new ParamGridBuilder()
      .addGrid(randomForest.maxBins, Array(25,28,31))
      .addGrid(randomForest.maxDepth, Array(4,6,8))
      .addGrid(randomForest.impurity, Array("entropy","gini"))
      .build()

    // Evaluator
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol(idxdLabelColName)

    // Cross validator
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    // Train model on train set
    val crossValidatorModel = cv.fit(allTrainingData)

    // Prediction on test set
    val predictions = crossValidatorModel.transform(allTestData)
    predictions.show()

  }
}
