package top.istyphoon.computing.easycomputing.total;

import top.istyphoon.template.AbstractEasyComputing;

/**.
 * 计算收藏的用户总数
 */
public class ReadTotalComputing extends AbstractEasyComputing {

  /**
   * . 设置当前方法使用的SQL语句
   *
   * @return 需要使用的SQL语句
   */
  @Override
  protected String setSql() {
    return "select count(type) from userBehavior where type=1";
  }

  /**
   * . 设置简单计算的输出路径
   *
   * @return 返回简单计算的输出路径
   */
  @Override
  protected String setOutputPath() {
    return "./output/read";
  }
}
