package edu.hubu

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object ListWordCount {
  def main(args: Array[String]): Unit = {

    // todo: 1. 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // todo: 2. Source：获取数据源

    val sourceDS: DataStream[String] = env.fromCollection(data = List("hadoop", "hdfs", "yarn", "flink", "spark", "hbase"))
    // todo: 3. Transformation：处理数据
    //          val resultDS:DataStream[String]=sourceDS.filter(new FilterFunction[String]{
    //            override def filter(value: String):Boolean={
    //              value.length>4&&value.startsWith("h")
    //            }
    //          })
    val resultDS: DataStream[String] = sourceDS.filter(value => value.length > 4 && value.startsWith("h"))
    // todo: 4. Sink：指定结果位置
    resultDS.print()
    // resultDS.writeAsText(path = "e:\\WordCount(可删)\\result.txt")
    // todo: 5. 启动执行
    env.execute(jobName = "filter job")
  }
}