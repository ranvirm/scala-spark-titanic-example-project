import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

// load all data here - do not do any data cleaning or transformations here - keep it raw
// add additional methods for individual datasets

object DataSourcer {

  def rawTrainData(sparkSession: SparkSession): DataFrame = {

    // load train data from local
    sparkSession.read.option("header", "true").csv("./src/main/resources/train.csv")

  }

  def rawTestData(sparkSession: SparkSession): DataFrame = {

    // load train data from local
    // populate label col - not included in raw test data
    sparkSession.read.option("header", "true").csv("./src/main/resources/test.csv")
      .withColumn("label", lit("0"))

  }

}
