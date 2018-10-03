
object ModelTrain {

  def main(args: Array[String]): Unit = {

    // create spark session
    val spark = SparkSessionCreator.sparkSessionCreate()

    // train data
    val rawTrainData = DataSourcer.rawTrainData(sparkSession = spark)

    // clean train data
    val cleanTrainData = DataCleaner.cleanData(dataFrame = rawTrainData)

    // feature data
    val featureTrainData = FeatureEngineering.featureData(dataFrame = cleanTrainData)

    // fitted pipeline
    val fittedPipeline = MachineLearning.pipelineFit(dataFrame = featureTrainData)

    // save fitted pipeline
    OutputSaver.pipelineSaver(pipelineModel = fittedPipeline)

  }

}
