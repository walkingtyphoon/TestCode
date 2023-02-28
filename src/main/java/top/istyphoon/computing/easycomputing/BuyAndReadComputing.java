package top.istyphoon.computing.easycomputing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import scala.Tuple3;
import top.istyphoon.template.AbstractComputing;

/**.
 * 统计商品排行前六的浏览和购买数量
 */
public class BuyAndReadComputing extends AbstractComputing {

  /**
   * . 设置当前方法使用的SQL语句
   *
   * @return 需要使用的SQL语句
   */
  @Override
  protected String setSql() {
    return "SELECT itemId,\n"
        + "       cast(SUM(CASE WHEN type = 1 THEN 1 ELSE 0 END) as signed) AS type_1_count,\n"
        + "       cast(SUM(CASE WHEN type = 4 THEN 1 ELSE 0 END) as signed) AS type_4_count\n"
        + "FROM userBehavior\n"
        + "GROUP BY itemId order by type_4_count+type_1_count desc limit 6";
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
      dataSet.map(new MapFunction<Row, Tuple3<Long,Long,Long>>() {
        @Override
        public Tuple3<Long, Long, Long> map(Row value) {
          return new Tuple3<>(
              Long.parseLong(value.getField(0).toString()),
              Long.parseLong(value.getField(1).toString()),
              Long.parseLong(value.getField(2).toString())
          );
        }
      }).writeAsText("./output/buyAndRead", FileSystem.WriteMode.OVERWRITE).setParallelism(100);
  }
}
