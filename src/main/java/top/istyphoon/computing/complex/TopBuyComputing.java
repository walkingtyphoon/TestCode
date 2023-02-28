package top.istyphoon.computing.complex;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import top.istyphoon.template.AbstractComputing;

/**
 * . 近三个月的顶级商品成交数量
 */
public class TopBuyComputing extends AbstractComputing {

  /**
   * . 设置当前方法使用的SQL语句
   *
   * @return 需要使用的SQL语句
   */
  @Override
  public String setSql() {
    return "SELECT itemId,\n"
        + "       CAST(SUM(CASE WHEN type = 1 THEN 1 ELSE 0 END) AS SIGNED) AS type_1_count,\n"
        + "       CAST(SUM(CASE WHEN type = 2 THEN 1 ELSE 0 END) AS SIGNED) AS type_2_count,\n"
        + "       CAST(SUM(CASE WHEN type = 3 THEN 1 ELSE 0 END) AS SIGNED) AS type_3_count,\n"
        + "       CAST(SUM(CASE WHEN type = 4 THEN 1 ELSE 0 END) AS SIGNED) AS type_4_count\n"
        + "FROM userBehavior\n"
        + "GROUP BY itemId\n"
        + "order by type_1_count+type_2_count+type_3_count+type_4_count desc limit 3";
  }

  /**
   * . 设置输出的数据格式类型 此处我们需要参考RowTypeInfo; BasicTypeInfo.XXXX_TYPE_INFO
   *
   * @return 输出的数据行类型
   */
  @Override
  public RowTypeInfo setOutputType() {
    return new RowTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
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
  public void count(DataSet<Row> dataSet) {
    dataSet.map(new MapFunction<Row, Tuple5<Long, Long, Long, Long, Long>>() {

          /**
           * The mapping method. Takes an element from the input data set and transforms it
           * into exactly one element.
           *
           * @param value The input value.
           * @return The transformed value
           */
          @Override
          public Tuple5<Long,Long, Long, Long, Long> map(Row value) {
            return new Tuple5<>(
                Long.parseLong(value.getField(0).toString()),
                Long.parseLong(value.getField(1).toString()),
                Long.parseLong(value.getField(2).toString()),
                Long.parseLong(value.getField(3).toString()),
                Long.parseLong(value.getField(4).toString())
                );
          }
        }).writeAsText("./output/topGoodMessage", FileSystem.WriteMode.OVERWRITE)
        .setParallelism(100);
  }
}
