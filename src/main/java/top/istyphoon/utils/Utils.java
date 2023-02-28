package top.istyphoon.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * . 存储数据库配置信息
 */
public class Utils {

  /**
   * . 获取数据的配置信息 其中位置分别包括 url username password sql
   *
   * @return 存储配置信息的集合
   * @throws IOException 可能存在的问题
   */
  public static ArrayList<String> getProperties() throws IOException {
    // 读取配置文件
    Properties prop = new Properties();
    // 设置数据库配置文件信息的位置
    prop.load(Utils.class.getResourceAsStream("/dataBase.properties"));
    // 获取配置文件中的连接信息
    ArrayList<String> strings = new ArrayList<>();
    // 添加配置信息到集合
    strings.add(prop.getProperty("database.url"));
    strings.add(prop.getProperty("database.username"));
    strings.add(prop.getProperty("database.password"));
    // 返回存储信息的集合
    return strings;
  }

  /**
   * . 读取数据，并将其转换为json格式
   */
  public static void convertData() {

    // 从文件中读取数据
    String filename = "./output/data.txt";
    String data = "";
    try {
      data = Files.readString(Paths.get(filename));
    } catch (IOException e) {
      e.printStackTrace();
    }

    String[] lines = data.split("\n");
    Map<String, Object> map = new HashMap<>();
    List<Object> outputList = new ArrayList<>();
    List<Object> topGoodMessageList = new ArrayList<>();
    List<Object> buyAndReadList = new ArrayList<>();

    String lastKey = "";

    for (String line : lines) {
      String[] keyValue = line.split(":");
      String key = keyValue[0];
      String value = "";
      if (keyValue.length > 1) {
        value = keyValue[1];
      }

      switch (key) {
        case "output":
          String[] outputValues = value.replaceAll("[()]", "").split(",");
          outputList.add(Arrays.asList(
              Long.parseLong(outputValues[0]),
              Long.parseLong(outputValues[1])
          ));
          lastKey = key;
          break;
        case "topGoodMessage":
          String[] topGoodMessageValues = value.replaceAll("[()]", "").split(",");
          topGoodMessageList.add(Arrays.asList(
              Long.parseLong(topGoodMessageValues[0]),
              Long.parseLong(topGoodMessageValues[1]),
              Long.parseLong(topGoodMessageValues[2]),
              Long.parseLong(topGoodMessageValues[3]),
              Long.parseLong(topGoodMessageValues[4])
          ));
          lastKey = key;
          break;
        case "buyAndRead":
          String[] buyAndReadValues = value.replaceAll("[()]", "").split(",");
          buyAndReadList.add(Arrays.asList(
              Long.parseLong(buyAndReadValues[0]),
              Long.parseLong(buyAndReadValues[1]),
              Long.parseLong(buyAndReadValues[2])
          ));
          lastKey = key;
          break;
        default:
          // 处理括号包含的数据
          if (key.startsWith("(")) {
            String[] values = key.replaceAll("[()]", "").split(",");
            switch (lastKey) {
              case "output":
                outputList.add(Arrays.asList(
                    Long.parseLong(values[0]),
                    Long.parseLong(values[1])
                ));
                break;
              case "topGoodMessage":
                topGoodMessageList.add(Arrays.asList(
                    Long.parseLong(values[0]),
                    Long.parseLong(values[1]),
                    Long.parseLong(values[2]),
                    Long.parseLong(values[3]),
                    Long.parseLong(values[4])
                ));
                break;
              case "buyAndRead":
                buyAndReadList.add(Arrays.asList(
                    Long.parseLong(values[0]),
                    Long.parseLong(values[1]),
                    Long.parseLong(values[2])
                ));
                break;
            }
            break;
          }
          map.put(key, Long.parseLong(value));
          break;
      }
    }

    map.put("output", outputList);
    map.put("topGoodMessage", topGoodMessageList);
    map.put("buyAndRead", buyAndReadList);

    ObjectMapper mapper = new ObjectMapper();
    String json = null;
    try {
      json = mapper.writeValueAsString(map);
    } catch (Exception e) {
      e.printStackTrace();
    }
    // 转换数据并写入到文件中
    try {
      Files.write(Paths.get("./output/data.json"), json.getBytes());
    } catch (
        IOException e) {
      e.printStackTrace();
    }

  }
}
