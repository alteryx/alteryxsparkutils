import org.apache.spark.AccumulatorParam
import org.apache.spark.SparkContext

object AlteryxSparkUtils {

  //This "worked" when defined as a class but not as an object.  But try again as an object.
  object AlteryxMessageLogAccumulator extends AccumulatorParam[String] {
    def zero(initialValue : String) : String = { "" }
    def addInPlace(s1 : String, s2 : String) : String = { s"$s1\n$s2" }
  }

  def attachAccumulator(sc: SparkContext) = { sc.accumulator("")(AlteryxMessageLogAccumulator) }
}
