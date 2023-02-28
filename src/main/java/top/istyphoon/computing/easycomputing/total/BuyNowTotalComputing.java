package top.istyphoon.computing.easycomputing.total;

import top.istyphoon.template.AbstractEasyComputing;

/**.
 * 实现简单计算当前已经完成购买的数量
 */
public class BuyNowTotalComputing extends AbstractEasyComputing {

  /**
   * . 设置当前方法使用的SQL语句
   *
   * @return 需要使用的SQL语句
   */
  @Override
  protected String setSql() {
    return "SELECT COUNT(type) "
        + "FROM userBehavior "
        + "WHERE DATE(saveTime) = (SELECT MAX(DATE(saveTime)) FROM userBehavior) "
        + "  and type = 4 "
        + "GROUP BY type";
  }

  /**
   * . 设置简单计算的输出路径
   *
   * @return 返回简单计算的输出路径
   */
  @Override
  protected String setOutputPath() {
    return "./output/buyNowTotal";
  }
}
