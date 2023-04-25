package edu.hubu

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Date
import java.text.SimpleDateFormat

case class Score(id:String,course:String,temp:Double,time:Long)
object ScoreDemo {
  def main(args: Array[String]): Unit = {
    // todo: 1. 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // todo: 2. Source：获取数据源
    val sourceDS: DataStream[String] = env.readTextFile("E:\\学习\\大三上\\大数据\\新建文件夹 (2)\\test project\\src\\main\\resources\\score.txt")
    // todo: 3. Transformation：处理数据
    val mapDS: DataStream[Score] = sourceDS.map(data => {
      val arr: Array[String] = data.split(" ")
      Score(arr(0), arr(1), arr(2).toDouble, arr(3).toLong)
    })
      .assignAscendingTimestamps(_.time*1000L)

    val resultDS: DataStream[Score] = mapDS.keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(15)))
      .maxBy("temp")

    // todo: 4. Sink：指定结果位置
    resultDS.print()
    // todo: 5. 启动执行
    env.execute("stream job")
  }
}
