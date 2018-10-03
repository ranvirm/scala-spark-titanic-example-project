import org.apache.spark.sql.SparkSession

// Create or retrieve a spark session here
// Change the master to 'yarn' when running on the Asgard cluster

object SparkSessionCreator {

  def sparkSessionCreate(): SparkSession = {

    SparkSession
      .builder()
      .master("local[*]")
      .appName("scala-spark-titanic-example-project")
      .getOrCreate()

  }

}
