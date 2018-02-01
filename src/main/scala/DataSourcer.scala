import SparkSessionCreator.Spark
import org.apache.spark.sql.DataFrame

// load all data here - do not do any data cleaning or transformations here - keep it raw
// add additional methods for individual datasets

object DataSourcer {

  def RawData(): DataFrame = {

    // get spark session
    val spark = Spark()

    // load train data from local
    spark.read.option("header", "true").csv("./src/main/resources/train.csv")

  }

}
