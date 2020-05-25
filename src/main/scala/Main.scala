import com.spark.dev.CheckPoint
import com.spark.utility.SparkUtility

object Main {

  def main(args: Array[String]): Unit = {
    val spark=SparkUtility.init()
    println("hello lumiq")
    val AadhaarDF = SparkUtility.getAadharDF(spark)//.show(5,false)

    /**
      * checkpoints
      */
    println("\n Initialising Assignment1 \n")
    //CheckPoint.checkpoint1(spark,AadhaarDF)
    //CheckPoint.checkpoint2(spark,AadhaarDF)
    CheckPoint.checkpoint3(spark,AadhaarDF)
    //CheckPoint.checkpoint4(spark,AadhaarDF)
    CheckPoint.checkpoint5(spark,AadhaarDF)

    //println("\n Initialising Assignment2 \n")
    //CheckPoint.Good_Bad_DF(spark)

    spark.stop()
  }

}
