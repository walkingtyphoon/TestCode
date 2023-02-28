package top.istyphoon.template;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.types.Row;
import org.junit.ClassRule;
import top.istyphoon.utils.Utils;

/**
 * . 抽象计算的父类
 */
public abstract class AbstractComputing {

  // 创建Flink程序的配置对象
  static Configuration flinkConfig = new Configuration();

  /*.
    设置静态代码块初始化计算机使用的内存大小
   */
  static {
    flinkConfig.setInteger("taskmanager.numberOfTaskSlots", 8);
    flinkConfig.setString("taskmanager.memory.off-heap.size", "6g");
  }

  /**
   * . 设置Flink模拟节点及任务管理配置
   */
  @ClassRule
  public static MiniClusterResource miniClusterResource =
      new MiniClusterResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(10)
              .setNumberSlotsPerTaskManager(8).setShutdownTimeout(Time.milliseconds(2000))
              .build());

  /**
   * . 获取批处理执行环境
   */
  private ExecutionEnvironment getExecutionEnvironment() {
    return ExecutionEnvironment.createLocalEnvironment(flinkConfig);
  }

  /**
   * . 进行初始化操作的方法
   */
  private DataSet<Row> init() throws IOException {
    ExecutionEnvironment env = getExecutionEnvironment();
    JdbcInputFormat jdbcInputFormat = getJdbcInputFormat();
    return env.createInput(jdbcInputFormat).setParallelism(100);
  }

  /**
   * . 获取 JDBC 连接属性
   */
  private ArrayList<String> getProperties() throws IOException {
    return Utils.getProperties();
  }

  /**
   * . 获取 JDBC 连接格式化器
   */
  private JdbcInputFormat getJdbcInputFormat() throws IOException {
    ArrayList<String> properties = getProperties();
    String url = properties.get(0);
    String username = properties.get(1);
    String password = properties.get(2);

    return JdbcInputFormat.buildJdbcInputFormat()
        .setDrivername("com.mysql.cj.jdbc.Driver")
        .setDBUrl(url)
        .setUsername(username)
        .setPassword(password)
        .setQuery(setSql())
        .setRowTypeInfo(setOutputType())
        .setFetchSize(100)
        .finish();
  }


  /**
   * . 设置当前方法使用的SQL语句
   *
   * @return 需要使用的SQL语句
   */
  protected abstract String setSql();

  /**
   * . 设置输出的数据格式类型 此处我们需要参考RowTypeInfo; BasicTypeInfo.XXXX_TYPE_INFO
   *
   * @return 输出的数据行类型
   */
  protected abstract RowTypeInfo setOutputType();

  /**
   * . 对SQL执行后的数据集合进行操作
   *
   * @param dataSet 经过SQL执行后获取到的数据集合
   */
  protected abstract void count(DataSet<Row> dataSet);

  /**
   * . 删除因为高并行度产生的空文件
   *
   * @param file 存储文件的目录
   */
  private void close(File file) {
// 如果当前对象是一个目录
    if (file.isDirectory()) {
      // 获取目录下所有文件和子目录
      File[] files = file.listFiles();
      if (files != null) {
        // 遍历目录下所有文件和子目录
        for (File f : files) {
          // 如果当前对象是一个目录，则继续递归
          if (f.isDirectory()) {
            close(f);
          }
          // 如果当前对象是一个文件，判断是否为空文件
          else if (f.length() == 0) {
            // 如果是空文件则删除
            f.delete();
          }
        }
      }
    }
  }


  /**
   * . 获取执行后的结果
   *
   * @throws IOException 可能存在的异常信息
   */
  public final void display() throws Exception {
    // 获取SQL执行后的数据集
    DataSet<Row> init = init();
    try {
      // 进行数据操作
      count(init);
    } finally {
      // 触发计算
      init.collect();
      // 操作完成后删除因为高并行产生的空文件
      close(new File("./output"));
      // 合并数据文件
      mergeFiles();
    }
  }

  /**
   * 合并数据文件
   */
  private void mergeFiles() {
    String sourceDirPath = "./output"; // 合并的数据文件夹路径
    String mergedFilePath = "./output/data.txt"; // 合并后的文件路径
    Path mergedFile = Paths.get(mergedFilePath); // 创建 Path 对象，用于合并后的文件操作
    try (BufferedOutputStream bos = new BufferedOutputStream(
        Files.newOutputStream(mergedFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
      // 使用 try-with-resources 语句，创建 BufferedOutputStream 对象用于写入合并后的文件
      Files.walk(Paths.get(sourceDirPath)) // 使用 Files.walk() 方法遍历所有数据文件
          .filter(path -> !path.equals(mergedFile)) // 过滤掉合并后的文件本身，避免重复写入
          .filter(Files::isRegularFile) // 过滤掉非普通文件（如目录、符号链接等）
          .parallel() // 使用 parallel() 方法开启并行流，加快文件合并的速度
          .forEach(path -> {
            try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(
                path))) { // 使用 try-with-resources 语句，创建 BufferedInputStream 对象用于读取数据文件
              bos.write((path.getParent().getFileName() + ":").getBytes()); // 写入文件夹名称
              byte[] buffer = new byte[8192]; // 创建缓冲区
              int bytesRead;
              while ((bytesRead = bis.read(buffer)) != -1) { // 读取数据文件内容并写入合并后的文件
                bos.write(buffer, 0, bytesRead);
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
            try {
              Files.delete(path); // 删除已经合并的数据文件
              Path parent = path.getParent();
              if (parent != null && !parent.equals(Paths.get(sourceDirPath))) {
                Files.delete(parent);
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
