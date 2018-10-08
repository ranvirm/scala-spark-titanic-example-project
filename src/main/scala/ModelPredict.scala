import org.apache.spark.ml.PipelineModel

object ModelPredict {

  def main(args: Array[String]): Unit = {

    // create spark session
    val spark = SparkSessionCreator.sparkSessionCreate()

    // train data
    val rawTestData = DataSourcer.rawTestData(sparkSession = spark)

    // clean train data
    val cleanTestData = DataCleaner.cleanData(dataFrame = rawTestData)

    // feature data
    val featureTestData = FeatureEngineering.featureData(dataFrame = cleanTestData)

    // load fitted pipeline
    val fittedPipeline = PipelineModel.load("./pipelines/fitted-pipeline")

    // make predictions
    val predictions = fittedPipeline.transform(dataset = featureTestData)

    // save predictions
    // this saves a csv in the format required by kaggle
    // ie. only the passenger_id and survived cols
    OutputSaver.predictionsSaver(dataFrame = predictions)

  }

}
