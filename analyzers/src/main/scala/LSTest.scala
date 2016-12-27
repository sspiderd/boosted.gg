import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{LabeledPoint, StringIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by ilan on 12/25/16.
  */
object LSTest {

  case class Walla(label:Int, itemId:Int, category: String)

  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession
      .builder()
      .appName("name")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    val values = spark.sparkContext.parallelize(Seq[Walla](
      Walla(1, 1, "a"),
      Walla(0, 1, "b")))
      .toDF("label", "itemId", "category")

    //Categorize second
    var transformed = new StringIndexer().setInputCol("category").setOutputCol("categoryIdx").fit(values).transform(values)
    transformed = new StringIndexer().setInputCol("itemId").setOutputCol("itemIdx").fit(transformed).transform(transformed)


    val readyToFit = transformed.select($"label", $"itemIdx", $"categoryIdx").map(r => LabeledPoint(r.getInt(0).toDouble, Vectors.dense(r.getDouble(1), r.getDouble(2))))

    val model = new LogisticRegression().fit(readyToFit)

    println (model.coefficientMatrix)
  }

}
