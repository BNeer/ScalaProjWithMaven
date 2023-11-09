import org.apache.spark.sql.{SparkSession, DataFrame}

object ScalaApi {

  def main(args: Array[String]): Unit = {



    // Initialize a Spark session
    val spark = SparkSession.builder
      .appName("PostgresToHive")
      .enableHiveSupport()
      .getOrCreate()

    val dburl = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"

    // The value of postgresQuery will be passed to argument (.option) of line 15 below
    val postgresQuery = "(select * from b1) AS B1"

    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", dburl)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", postgresQuery)
      .option("user", "consultants")
      .option("password", "WelcomeItc@2022")
      .load()

    df.show()

    // Stop the Spark session
    spark.stop()

  }
}