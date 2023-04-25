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

// sensor_1,25.5,1547718199
case class Sensor(id:String,temp:Double,time:Long)
object StreamAPIDemo {
  def main(args: Array[String]): Unit = {
    // todo: 1. 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // todo: 2. Source：获取数据源
    val sourceDS: DataStream[String] = env.readTextFile("E:\\学习\\大三上\\大数据\\新建文件夹 (2)\\test project\\src\\main\\resources\\sensordata.txt")
    // todo: 3. Transformation：处理数据
    val mapDS: DataStream[Sensor] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      Sensor(arr(0), arr(1).toDouble, arr(2).toLong)
    })
      .assignAscendingTimestamps(_.time*1000L)

//    val resultDS: DataStream[Sensor] = mapDS.keyBy(_.id)
//      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
////      .max("temp")
//      .maxBy("temp")  // 推荐

////     统计窗口内记录的条数
//    val resultDS: DataStream[Int] = mapDS.keyBy(_.id)
//      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//      .aggregate(new MyCountAggFunc)

    // 统计窗口内温度的平均值
    val resultDS: DataStream[String] = mapDS.keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .aggregate(new MyAvgAggFunc, new MyAvgWindowFunc)
    // todo: 4. Sink：指定结果位置
    resultDS.print()
    // todo: 5. 启动执行
    env.execute("stream job")
  }
}
class MyAvgWindowFunc extends WindowFunction[Double,String,String,TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Double], out: Collector[String]): Unit = {
    val start: String = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(window.getStart))
    val end: String = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(window.getEnd))
    val avgTemp: Double = input.iterator.next()
    out.collect("窗口的开始时间："+start+"--->窗口的结束时间："+end+"传感器："+key+"平均温度："+avgTemp)
  }
}
class MyAvgAggFunc extends AggregateFunction[Sensor,(Double,Int),Double] {
  override def createAccumulator(): (Double, Int) = (0,0)

  override def add(value: Sensor, accumulator: (Double, Int)): (Double, Int) = {
    (accumulator._1+value.temp,accumulator._2+1)
  }

  override def getResult(accumulator: (Double, Int)): Double = {
    accumulator._1/accumulator._2
  }

  override def merge(a: (Double, Int), b: (Double, Int)): (Double, Int) = {
    (a._1+b._1,a._2+b._2)
  }
}


class MyCountAggFunc extends AggregateFunction[Sensor,Int,Int] {
  override def createAccumulator(): Int = 0

  override def add(value: Sensor, accumulator: Int): Int = accumulator+1

  override def getResult(accumulator: Int): Int = accumulator

  override def merge(a: Int, b: Int): Int = a+b
}