package top.istyphoon.computing.easycomputing;

import top.istyphoon.template.AbstractEasyComputing;

/**
 * . 获取平均访问次数
 */
public class AverageVisitComputing extends AbstractEasyComputing {

  /**
   * . 设置当前方法使用的SQL语句
   *
   * @return 需要使用的SQL语句
   */
  @Override
  protected String setSql() {
    return "SELECT CAST(((MAX(saveTime) - MIN(saveTime)) / COUNT(userId)) AS SIGNED) FROM userBehavior;";
  }

  /**
   * . 设置简单计算的输出路径
   *
   * @return 返回简单计算的输出路径
   */
  @Override
  protected String setOutputPath() {
    return "./output/average";
  }
}
