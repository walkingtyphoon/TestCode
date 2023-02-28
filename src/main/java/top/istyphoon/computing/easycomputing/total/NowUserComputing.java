package top.istyphoon.computing.easycomputing.total;

import top.istyphoon.template.AbstractEasyComputing;

/**
 * . 获取当前的用户数量
 */
public class NowUserComputing extends AbstractEasyComputing {

  /**
   * . 设置当前方法使用的SQL语句
   *
   * @return 需要使用的SQL语句
   */
  @Override
  protected String setSql() {
    return "SELECT COUNT(userId)\n"
        + "FROM userBehavior\n"
        + "WHERE DATE(saveTime) = (SELECT MAX(DATE(saveTime)) FROM userBehavior)";
  }


  /**
   * . 设置简单计算的输出路径
   *
   * @return 返回简单计算的输出路径
   */
  @Override
  protected String setOutputPath() {
    return "./output/nowUser";
  }
}
