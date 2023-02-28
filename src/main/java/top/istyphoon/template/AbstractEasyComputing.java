package top.istyphoon.template;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;

/**.
 * 实现简单计算的模版方法
 */
public abstract class AbstractEasyComputing extends AbstractComputing{

  /**
   * . 设置输出的数据格式类型 此处我们需要参考RowTypeInfo; BasicTypeInfo.XXXX_TYPE_INFO
   *
   * @return 输出的数据行类型
   */
  @Override
  protected RowTypeInfo setOutputType() {
    return new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO);
  }

  /**
   * . 对SQL执行后的数据集合进行操作
   *
   * @param dataSet 经过SQL执行后获取到的数据集合
   */
  @Override
  protected void count(DataSet<Row> dataSet) {
    dataSet.map((MapFunction<Row, Long>)
            x -> Long.parseLong(x.getField(0).toString())
        )
        .writeAsText(setOutputPath(), FileSystem.WriteMode.OVERWRITE).setParallelism(100);
  }

  /**.
   * 设置简单计算的输出路径
   * @return 返回简单计算的输出路径
   */
  protected abstract String setOutputPath();

}
