/**
	* Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
	*
	* Licensed under the Apache License, Version 2.0 (the "License");
	* you may not use this file except in compliance with the License.
	* You may obtain a copy of the License at
	*
	*    http://www.apache.org/licenses/LICENSE-2.0
	*
	* Unless required by applicable law or agreed to in writing, software
	* distributed under the License is distributed on an "AS IS" BASIS,
	* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	* See the License for the specific language governing permissions and
	* limitations under the License.
	**/

package org.zuinnote.spark.bitcoin.example


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.conf._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.graphx._
import org.zuinnote.spark.bitcoin.example.SparkScalaBitcoinTransactionGraph.jobTop5AddressInput
//import org.apache.spark.sql.SQLContext.implicits._
//import SQLContext.implicits._

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import org.apache.spark.sql.Column
import org.zuinnote.hadoop.bitcoin.format.common._
import org.zuinnote.hadoop.bitcoin.format.mapreduce._


/**
	* Author: Jörn Franke <zuinnote@gmail.com>
	*
	*/

/**

	*
	* Constructs a graph out of Bitcoin Transactions.
	*
	* Vertex: Bitcoin address (only in the old public key and the new P2P hash format)
	* Edge: A directed edge from one source Bitcoin address to a destination Bitcoin address (A link between the input of one transaction to the originating output of another transaction via the transaction hash and input/output indexes)
	*
	* Problem to solve: Find the top 5 bitcoin addresses with the highest number of inputs from other bitcoin addresses
	*/

object SparkScalaBitcoinTransactionGraph {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Spark-Scala-Graphx BitcoinTransaction Graph (hadoopcryptoledger)")
		val sc=new SparkContext(conf)
		val hadoopConf = new Configuration();
		val spark = org.apache.spark.sql.SparkSession.builder()
			.getOrCreate()
		//spark.conf.set("spark.sql.crossJoin.enabled", "true")
		import spark.implicits._
		hadoopConf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9");
		jobTop5AddressInput(sc,hadoopConf,args(0),args(1),args(2),args(3))
		sc.stop()
	}

	def jobTop5AddressInput(sc: SparkContext, hadoopConf: Configuration, inputFile: String, outputFile: String,back_type: String,central_addreess:String): Unit = {
		val bitcoinBlocksRDD = sc.newAPIHadoopFile(inputFile, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock],hadoopConf)
		// extract a tuple per transaction containing Bitcoin destination address, the input transaction hash, the input transaction output index, and the current transaction hash, the current transaction output index, a (generated) long identifier
		val bitcoinTransactionTuples = bitcoinBlocksRDD.flatMap(hadoopKeyValueTuple => extractTransactionData(hadoopKeyValueTuple._2))

		val rowRDD = bitcoinTransactionTuples.map(p => Row(p._1, p._2, p._3, p._4, p._5, p._6,p._7))

		val transactionSchema = StructType(
			Array(
				StructField("dest_address", StringType, true),
				StructField("curr_trans_input_hash", BinaryType, false),
				StructField("curr_trans_input_output_idx", LongType, false),
				StructField("curr_trans_hash", BinaryType, false),
				StructField("curr_trans_output_idx", LongType, false),
				StructField("timestamp", IntegerType, false),
				StructField("value",LongType,false)
			)
		)
		val sqlContext= new SQLContext(sc)
		//import sqlContext.implicits._
		import sqlContext.implicits._
		import org.apache.spark.sql.functions.udf
		//val toString = udf((payload: Array[Byte]) => new String(payload))
		//def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))
		val btcDF = sqlContext.createDataFrame(rowRDD, transactionSchema)
		//var transhash=btcDF.filter($"dest_address".equalTo("bitcoinaddress_"+central_addreess)).select($"curr_trans_hash").distinct
		//val sameTranscation=transhash.join(btcDF,transhash("curr_trans_hash")===btcDF("curr_trans_hash"))
		//val sameTranscation=btcDF.filter(centralTranscations("curr_trans_hash")===btcDF("curr_trans_hash"))
		//centralTranscations.show(10)

		val sourceDF=btcDF.select($"dest_address".alias("source_address"), $"curr_trans_input_hash".alias("source_trans_input_hash"),$"curr_trans_input_output_idx".alias("source_trans_input_output_idx"),$"curr_trans_hash".alias("source_trans_hash"), $"curr_trans_output_idx".alias("source_trans_output_idx"),$"timestamp".alias("source_timestamp"),$"value".alias("source_value"))
		val joined_degree1=btcDF.join(sourceDF,btcDF("curr_trans_input_hash")===sourceDF("source_trans_hash")&&btcDF("curr_trans_input_output_idx")===sourceDF("source_trans_output_idx"))
		joined_degree1.show(10)
		val data=joined_degree1.select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp",$"curr_trans_hash".alias("string_hash")).distinct
		data.write.parquet(outputFile+"/"+back_type+"/dataWithHash")
//		val central_data=data.filter($"dest_address".equalTo("bitcoinaddress_"+central_addreess) ||$"source_address".equalTo("bitcoinaddress_"+central_addreess))
//		central_data.show(10)
//		val central_hash=central_data.select($"string_hash".alias("central_hash"))
//		val ralevent_central=central_hash.join(data,central_hash("central_hash")===data("string_hash"))
//		ralevent_central.select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp").distinct.write.format("com.databricks.spark.csv").option("header", "true").save(outputFile+"/"+back_type+"/degree1")
//
//		val addresses_degree1=ralevent_central.select($"source_address".alias("degree1_address")).distinct
//		val asinput_degree1=addresses_degree1.join(data,data("source_address")===addresses_degree1("degree1_address")).select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp",$"string_hash")
//		val asoutput_degree1=addresses_degree1.join(data,data("dest_address")===addresses_degree1("degree1_address")).select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp",$"string_hash")
//		val degree1=asinput_degree1.union(asoutput_degree1).toDF
//		val degree1_hash=degree1.select($"string_hash".alias("central_hash"))
//		val ralevent_degree1=degree1_hash.join(data,degree1_hash("central_hash")===data("string_hash"))
//		ralevent_degree1.select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp").distinct.write.format("com.databricks.spark.csv").option("header", "true").save(outputFile+"/"+back_type+"/degree2")

		//		val sameDest1=joined_degree1.select($"source_address".alias("dest_address")).distinct
//		val sameTransaction1=sameDest1.join(btcDF,sameDest1("dest_address")===btcDF("dest_address")).select($"curr_trans_hash")
//		val same_degree1=sameTransaction1.join(btcDF,sameTransaction1("curr_trans_hash")===btcDF("curr_trans_hash"))
//		//val same_degree1=source_degree1.join(btcDF,source_degree1("curr_trans_hash")===btcDF("curr_trans_hash"))
//
//		val joined_degree2=same_degree1.join(sourceDF,same_degree1("curr_trans_input_hash")===sourceDF("source_trans_hash")&&same_degree1("curr_trans_input_output_idx")===sourceDF("source_trans_output_idx"))
//		joined_degree2.show(10)
//		joined_degree2.select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp").distinct.write.format("com.databricks.spark.csv").option("header", "true").save(outputFile+"/"+back_type+"/degree2.csv")
//
//		val sameDest2=joined_degree2.select($"source_address".alias("dest_address")).distinct
//		val sameTransaction2=sameDest2.join(btcDF,sameDest2("dest_address")===btcDF("dest_address")).select($"curr_trans_hash")
//		val same_degree2=sameTransaction2.join(btcDF,sameTransaction2("curr_trans_hash")===btcDF("curr_trans_hash"))
//		//val same_degree1=source_degree1.join(btcDF,source_degree1("curr_trans_hash")===btcDF("curr_trans_hash"))
//
//		val joined_degree3=same_degree2.join(sourceDF,same_degree2("curr_trans_input_hash")===sourceDF("source_trans_hash")&&same_degree2("curr_trans_input_output_idx")===sourceDF("source_trans_output_idx"))
//		joined_degree3.show(10)
//		joined_degree3.select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp").distinct.write.format("com.databricks.spark.csv").option("header", "true").save(outputFile+"/"+back_type+"/degree3.csv")
//
//
		//		val source_degree2=joined_degree2.select($"source_address".alias("dest_address"), $"source_trans_input_hash".alias("curr_trans_input_hash"),$"source_trans_input_output_idx".alias("curr_trans_input_output_idx"),$"source_trans_hash".alias("curr_trans_hash"), $"source_trans_output_idx".alias("curr_trans_output_idx"),$"source_timestamp".alias("timestamp"),$"source_value".alias("value"))
//		val same_degree2=btcDF.filter(source_degree2("curr_trans_hash")===btcDF("curr_trans_hash"))
//		val joined_degree3=same_degree2.join(sourceDF,same_degree2("curr_trans_input_hash")===sourceDF("source_trans_hash")&&same_degree2("curr_trans_input_output_idx")===sourceDF("source_trans_output_idx"))
//		joined_degree2.show(10)
//		joined_degree2.select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp").distinct.write.format("com.databricks.spark.csv").option("header", "true").save(outputFile+"/"+back_type+"/degree3.csv")
//
//		val source_degree3=joined_degree3.select($"source_address".alias("dest_address"), $"source_trans_input_hash".alias("curr_trans_input_hash"),$"source_trans_input_output_idx".alias("curr_trans_input_output_idx"),$"source_trans_hash".alias("curr_trans_hash"), $"source_trans_output_idx".alias("curr_trans_output_idx"),$"source_timestamp".alias("timestamp"),$"source_value".alias("value"))
//		val same_degree3=btcDF.filter(source_degree3("curr_trans_hash")===btcDF("curr_trans_hash"))
//
//		val joined_degree4=same_degree3.join(sourceDF,same_degree3("curr_trans_input_hash")===sourceDF("source_trans_hash")&&same_degree3("curr_trans_input_output_idx")===sourceDF("source_trans_output_idx"))
//		joined_degree2.show(10)
//		joined_degree2.select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp").distinct.write.format("com.databricks.spark.csv").option("header", "true").save(outputFile+"/"+back_type+"/degree4.csv")
	}

	// extract relevant data
	def extractTransactionData(bitcoinBlock: BitcoinBlock): Array[(String,Array[Byte],Long,Array[Byte], Long,Int,Long)] = {
		// first we need to determine the size of the result set by calculating the total number of inputs multiplied by the outputs of each transaction in the block
		val transactionCount= bitcoinBlock.getTransactions().size()
		var resultSize=0
		for (i<-0 to transactionCount-1) {
			resultSize += bitcoinBlock.getTransactions().get(i).getListOfInputs().size()*bitcoinBlock.getTransactions().get(i).getListOfOutputs().size()
		}
		// then we can create a tuple for each transaction input: Destination Address (which can be found in the output!), Input Transaction Hash, Current Transaction Hash, Current Transaction Output
		// as you can see there is no 1:1 or 1:n mapping from input to output in the Bitcoin blockchain, but n:m (all inputs are assigned to all outputs), cf. https://en.bitcoin.it/wiki/From_address
		val result:Array[(String,Array[Byte],Long,Array[Byte], Long,Int,Long)]=new Array[(String,Array[Byte],Long,Array[Byte],Long,Int,Long)](resultSize)
		var resultCounter: Int = 0
		for (i <- 0 to transactionCount-1) { // for each transaction
			val currentTransaction=bitcoinBlock.getTransactions().get(i)
			val currentTransactionHash=BitcoinUtil.getTransactionHash(currentTransaction)
			for (j <-0 to  currentTransaction.getListOfInputs().size()-1) { // for each input
				val currentTransactionInput=currentTransaction.getListOfInputs().get(j)
				val currentTransactionInputHash=currentTransactionInput.getPrevTransactionHash()
				val currentTransactionInputOutputIndex=currentTransactionInput.getPreviousTxOutIndex()
				for (k <-0 to currentTransaction.getListOfOutputs().size()-1) {
					val currentTransactionOutput=currentTransaction.getListOfOutputs().get(k)
					var currentTransactionOutputIndex=k.toLong
					result(resultCounter)=(BitcoinScriptPatternParser.getPaymentDestination(currentTransactionOutput.getTxOutScript()),currentTransactionInputHash,currentTransactionInputOutputIndex,currentTransactionHash,currentTransactionOutputIndex,bitcoinBlock.getTime(),currentTransactionOutput.getValue().longValue())
					resultCounter+=1
				}
			}

		}
		result;
	}


}
object extractTransactionGraph{
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Spark-Scala-Graphx BitcoinTransaction Graph (hadoopcryptoledger)")
		val sc=new SparkContext(conf)
		val hadoopConf = new Configuration();

		//spark.conf.set("spark.sql.crossJoin.enabled", "true")
		hadoopConf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9");
		degree2graph(sc,args(0),args(1),args(2),args(3))
		sc.stop()
	}
	def degree2graph(sc: SparkContext, inputFile: String, outputFile: String,back_type: String,central_addreess:String): Unit={
		val spark = org.apache.spark.sql.SparkSession.builder()
			.getOrCreate()
		import spark.implicits._
		val data=spark.read.parquet(inputFile)
		val central_data=data.filter($"dest_address".equalTo("bitcoinaddress_"+central_addreess) ||$"source_address".equalTo("bitcoinaddress_"+central_addreess))
		central_data.show(10)
		val central_hash=central_data.select($"string_hash".alias("central_hash"))
		val ralevent_central=central_hash.join(data,central_hash("central_hash")===data("string_hash"))
		ralevent_central.select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp").distinct.write.format("com.databricks.spark.csv").option("header", "true").save(outputFile+"/"+back_type+"/degree1")

		val addresses_degree1=ralevent_central.select($"source_address".alias("degree1_address")).distinct
		val asinput_degree1=addresses_degree1.join(data,data("source_address")===addresses_degree1("degree1_address")).select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp",$"string_hash")
		val asoutput_degree1=addresses_degree1.join(data,data("dest_address")===addresses_degree1("degree1_address")).select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp",$"string_hash")
		val degree1=asinput_degree1.union(asoutput_degree1).toDF
		val degree1_hash=degree1.select($"string_hash".alias("central_hash"))
		val ralevent_degree1=degree1_hash.join(data,degree1_hash("central_hash")===data("string_hash"))
		ralevent_degree1.select($"dest_address",$"value",$"source_address",$"source_value",$"timestamp").distinct.write.format("com.databricks.spark.csv").option("header", "true").save(outputFile+"/"+back_type+"/degree2")	}
}

/**
	* Helper class to make byte arrays comparable
	*
	*
	*/
class ByteArray(val bArray: Array[Byte]) extends Serializable {
	override val hashCode = bArray.deep.hashCode
	override def equals(obj:Any) = obj.isInstanceOf[ByteArray] && obj.asInstanceOf[ByteArray].bArray.deep == this.bArray.deep
}

