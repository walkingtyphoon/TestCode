package top.istyphoon.computing.complex;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import top.istyphoon.template.AbstractComputing;

/**
 * . 获取排行榜前十的商品数据
 */
public class TopGoodsComputing extends AbstractComputing {


  /**
   * . 设置当前方法使用的SQL语句
   *
   * @return 需要使用的SQL语句
   */
  @Override
  protected String setSql() {
    return "select itemId,count(userId) as"
        + " userCount from userBehavior where type=1"
        + " group by itemId order by userCount desc limit 10";
  }

  /**
   * . 设置输出的数据格式类型 此处我们需要参考RowTypeInfo; BasicTypeInfo.XXXX_TYPE_INFO
   *
   * @return 输出的数据行类型
   */
  @Override
  protected RowTypeInfo setOutputType() {
    return new RowTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO
    );
  }

  /**
   * . 对SQL执行后的数据集合进行操作
   *
   * @param dataSet 经过SQL执行后获取到的数据集合
   */
  @Override
  protected void count(DataSet<Row> dataSet) {
    // 对数据进行处理
    dataSet.map(
        new MapFunction<Row, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> map(Row value) {
            return Tuple2.of(
                Long.parseLong(value.getField(0).toString()),
                Long.parseLong(value.getField(1).toString())
            );
          }
        }
    ).writeAsText("./output", FileSystem.WriteMode.OVERWRITE)
        .setParallelism(100);
  }
}
