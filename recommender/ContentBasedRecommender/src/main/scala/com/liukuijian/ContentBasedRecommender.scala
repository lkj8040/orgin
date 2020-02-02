package com.liukuijian

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix


case class MongoConfig(uri: String, db:String)

case class Movie(mid:Int, name:String, descri:String,
                 timelong:String, issue:String, shoot:String,
                 language:String, genres:String, actors:String, directors:String)


case class Recommendation(mid:Int, score:Double)

case class MovieRecs(mid:Int,recs:Seq[Recommendation])

object ContentBasedRecommender {

    val MONGODB_MOVIE_COLLECTION = "Movie"


    val MOVIE_RECS = "Content_Movie_Recs"

    def main(args: Array[String]): Unit = {
        //将movie表读进来
        val config = Map(
            "spark.core" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop101:27017/recommender",
            "mongo.db" -> "recommender"
        )
        val conf: SparkConf = new SparkConf().setMaster(config("spark.core")).setAppName("OfflineRecommender")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        import spark.implicits._

        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        val movieDF: DataFrame =
                spark
                    .read
                    .option("uri", mongoConfig.uri)
                    .option("collection", MONGODB_MOVIE_COLLECTION)
                    .format("com.mongodb.spark.sql")
                    .load()
                    .as[Movie]
                    .rdd
                    .map(movie => (movie.mid,movie.name,movie.genres))
                    .map {case (mid, name, genres) => (mid, name, genres.map(c => if(c == '|') ' ' else c))}
                    .toDF("mid","name","genres")
                    .cache()

        //实例化一个分词器,作用是新增一列，这列记录了[word1, word2,word3]
        val tokenizer: Tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")

        //用分词器做转换
        val wordsData: DataFrame = tokenizer.transform(movieDF)

        //定义一个HashTF工具,作用是新增一列，这列记录了[特征数, [维度不为零的索引], [对应的统计值]]
        val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

        //用HashTF做处理
        val featurizedData: DataFrame = hashingTF.transform(wordsData)

        //逆词频分析,记录了[特征数,[维度不为零索引],[对应的词频比例]]
        val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")

        //根据所有文章的单词，得到idf模型
        val iDFModel: IDFModel = idf.fit(featurizedData)

        //用tf-idf计算得到新的特征矩阵
        val rescaledData: DataFrame = iDFModel.transform(featurizedData)

        //取出特征向量
        val movieFeatures: RDD[(Int, DoubleMatrix)] =
            rescaledData
                .map {
                    //稀疏向量
                    case row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
                }
                .rdd
                .map(x => {
                    (x._1, new DoubleMatrix(x._2))
                })

        //对所有电影俩俩计算他们的相似度，先做笛卡尔积
        val movieRecs: RDD[MovieRecs] =
            movieFeatures.cartesian(movieFeatures)
                    .filter{
                        //把自己跟自己的配对过滤掉
                        case (a, b) => a._1 != b._1
                    }
                    .map{
                        case (a,b) =>{
                            val simScore: Double = this.consinSim(a._2,b._2)
                            (a._1,(b._1,simScore))
                        }
                    }
                    .filter(_._2._2 > 0.6) //过滤出相似度大于0.6
                    .groupByKey
                    .map{
                        case (mid, items) =>
                            MovieRecs(mid, items.toList.sortBy(-_._2).map(x => Recommendation(x._1,x._2)))
                    }
        movieRecs.toDF
                .write
                .option("uri",mongoConfig.uri)
                .option("collection",MOVIE_RECS)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save
    }

    //求向量余弦相似度
    def consinSim(matrix: DoubleMatrix, matrix1: DoubleMatrix): Double ={
        matrix.dot(matrix1) / (matrix.norm2() * matrix1.norm2())
    }
}
