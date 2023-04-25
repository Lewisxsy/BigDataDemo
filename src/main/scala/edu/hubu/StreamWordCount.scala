package edu.hubu

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // todo: 1. 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // todo: 2. Source：获取数据源
    val sourceDS: DataStream[String] = env.readTextFile("E:\\学习\\大三上\\大数据\\新建文件夹 (2)\\test project\\src\\main\\resources\\word.txt")

    // todo: 3. Transformation：处理数据
    val resultDS: DataStream[(String, Int)] = sourceDS.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
    // todo: 4. Sink：指定结果位置
    resultDS.print()
    // todo: 5. 启动执行
    env.execute("wordcount job")
  }
}
