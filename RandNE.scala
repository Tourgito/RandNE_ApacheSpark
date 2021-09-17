import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.linalg.{Vector,Vectors,DenseMatrix}
import org.apache.spark.mllib.linalg.distributed.{DistributedMatrix,RowMatrix,IndexedRowMatrix,IndexedRow}
import scala.math.sqrt
import org.apache.spark.sql.functions.{col, sum}

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import scala.io.Source
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import java.io.BufferedWriter

import org.apache.spark.sql.{SparkSession,Encoders,Encoder,DataFrame, Row, Column}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructType,StructField, ArrayType, LongType, IntegerType, DoubleType}



object RandNE{

 def main(args: Array[String]) {

     val spark = SparkSession
                        .builder()
                        .appName("RandNE")
                        .getOrCreate()

     spark.sparkContext.setLogLevel("Error")


     val q: Int = args(0).toInt
     val dimensionality: Int = args(1).toInt
     val graph: String = args(2)
     val numberOfPartitions: Int = args(3).toInt

     val e = args(4).toInt     

     val start:Long = System.nanoTime()
     val rn = new RandNE(spark, graph, dimensionality, q, List.range(1,q + 2).map(x => x.toDouble), numberOfPartitions)
     rn.execute
     val executionTime: Double = (System.nanoTime() - start).toDouble / 1000000000.0
     val writer = new BufferedWriter(new FileWriter("./executionsTime_RandNE.csv", true))

     if (q < e){
       if (q == 2)
        writer.write(numberOfPartitions.toString + ",")
     writer.write(executionTime.toString() + ",")
     }
     else {
     writer.write(executionTime.toString())
     writer.newLine()
     }
     writer.close()
     println("Xronos ekteleshs: " + executionTime + "\n")
     //rn.showEmmbendings
 }

 case class RandNE(var spark: SparkSession, path:String, dimensionality: Int, q: Int, weights:List[Double], numberOfPartitions: Int) {


    private val numberOfNodes: Int = this.getNumberOfGraphsNodes()


    private val adjMatrix_RDD: RDD[IndexedRow]  = this.spark.sparkContext.textFile(path,numberOfPartitions).map(nodeVector => {
                                                                                                              val nv: Array[Double] = nodeVector.split(",").map(x => x.toDouble)
                                                                                                              IndexedRow(nv(0).toLong, Vectors.dense(nv.slice(1,this.numberOfNodes + 1)))
                                                                                                              }
                                                                                              )
    
    // The adjejancy Matrix of the graph + the nodes' Id
    private val adjMatrix: IndexedRowMatrix = new IndexedRowMatrix(adjMatrix_RDD, this.numberOfNodes, this.numberOfNodes)

    // The final matrix that contains the nodes' emmbending
    private var U:DataFrame = null


    /*
    val item:RDD[Row] = this.spark.sparkContext.parallelize(Seq(Row(0, 1.0,2.0,3.0),
                                                       Row(1,1.0,2.0,3.0),
                                                       Row(2,1.0,2.0,3.0),
                                                       Row(3,1.0,2.0,3.0)), 3)                                                       
                                                      //.sortBy(x => x.getAs[Int](0))
                                                      */


    // Implements Algorithm 1 in https://arxiv.org/pdf/1805.02396.pdf
    def execute: Unit = {
      
      // A List that contains the {U_0, U_1,.....,U_q} Matrices
      // At initialization, the U_0 (Gaussian random matrix) is stored
      // Implements line 1 of Algorithm 1
      var U_list:List[RDD[Row]] = List(RandomRDDs.normalVectorRDD(spark.sparkContext,this.numberOfNodes,this.dimensionality)
                                                 .repartition(numberOfPartitions)
                                                 .zipWithIndex()
                                                 .map(x => Row.fromSeq(x._1.toArray.map(y => (1/sqrt(this.dimensionality)) * y.toDouble).+:(x._2.toInt)))
                                      )

      // Implements lines 3-5 of Algorithm 1
      for (i <- 1 to this.q){
          val U_i_Minus_1: DenseMatrix = new DenseMatrix(this.numberOfNodes, this.dimensionality, this.RDDToArray(U_list(i-1))) 
          U_list = U_list.:+(this.matricesMultiplication(U_i_Minus_1)) 
        }
      
      // A List that contains the {U_0 * a0, U_1 * a1,.....,U_q * aq} Matrices
      // Multiply each U_i matix with the corresponging weight
      val U_list_DF: List[DataFrame] = List.range(0, this.q + 1).map(x => multiply_U_Matrix_With_Weight(U_list(x), this.weights(x)))

      val colN: Array[Column] = U_list_DF(0).columns.slice(1,this.dimensionality + 1) // auto na to kanw alliws
                                            .map(x=> sum(x).as(x))
      
      // The final emmbendings of the Nodes
      // Implements line 6 of Algorithm 1
      this.U = U_list_DF.reduce(_.union(_))
                        .groupBy(col("Id"))
                        .agg(colN.head, colN.tail:_*)
                        .sort("Id")
      
      }

    def showEmmbendings: Unit = {
      this.U.show()
    }      
    
    // Multiply each cell a dataframe with a double number
    // U_i * a_i
    private def multiply_U_Matrix_With_Weight(U:RDD[Row], weight:Double): DataFrame = {
      val columns: List[Column] = List.range(1, this.dimensionality + 1).map(y => ((col(y.toString()) * weight).as(y.toString()))).+:(col("Id"))
      this.spark.createDataFrame(U,this.generate_UMatrix_DFSchema()).select(columns:_*)
    }

    
    // Cast an U_i matrix to a local array (without the id of the nodes) 
    // SOS: The returned Array must be able to be stored in the RAM of each cluster's machine
    private def RDDToArray(U:RDD[Row]): Array[Double] = {
      U.flatMap(x => x.toSeq
                      .zipWithIndex
                      .slice(1,this.dimensionality + 1)
                      .map(y =>  ( (x.getAs[Int](0), y._2), y._1.toString().toDouble))
                       )
       .sortBy(y => (y._1._2, y._1._1))
       .map(y => y._2)
       .collect() 
    }

    // Multiply 2 Matrices: The adjejency Matrix (IndexedRowMatrix) and an U_i Matrix (DenseMatrix) )
    // implements the Matrices multiplication in line 4 in https://arxiv.org/pdf/1805.02396.pdf
    private def matricesMultiplication(U:DenseMatrix): RDD[Row] = {

      this.adjMatrix.multiply(U).rows
                                .repartition(numberOfPartitions)
                                .map((x => Row.fromSeq(x.vector.toArray.+:(x.index.toInt))))
    }

    // Returns the number of nodes in the graph
    private def getNumberOfGraphsNodes(): Int = {
        Source.fromFile(path).bufferedReader().readLine().split(",").length - 1
    }


    private def generate_UMatrix_DFSchema() : StructType = {
      StructType(List.range(1,this.dimensionality + 1).map(x => StructField(x.toString(), DoubleType, false)).+:(StructField("Id",IntegerType,false)))
    }



 }

}

