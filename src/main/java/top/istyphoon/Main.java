package top.istyphoon;

import top.istyphoon.computing.complex.TopBuyComputing;
import top.istyphoon.computing.complex.TopGoodsComputing;
import top.istyphoon.computing.easycomputing.AverageVisitComputing;
import top.istyphoon.computing.easycomputing.BuyAndReadComputing;
import top.istyphoon.computing.easycomputing.total.BuyNowTotalComputing;
import top.istyphoon.computing.easycomputing.total.BuyTotalComputing;
import top.istyphoon.computing.easycomputing.total.CartTotalComputing;
import top.istyphoon.computing.easycomputing.total.FavoriteTotalComputing;
import top.istyphoon.computing.easycomputing.total.NowUserComputing;
import top.istyphoon.computing.easycomputing.total.ReadTotalComputing;
import top.istyphoon.computing.easycomputing.total.TotalComputing;
import top.istyphoon.template.AbstractComputing;
import top.istyphoon.utils.Utils;

public class Main {

  public static void main(String[] args) throws Exception {

    // 实现计算用户总数
    AbstractComputing abstractComputing = new ReadTotalComputing();
    abstractComputing.display();
    System.out.println("read total end");

    abstractComputing = new FavoriteTotalComputing();
    abstractComputing.display();
    System.out.println("favorite total end");

    abstractComputing = new CartTotalComputing();
    abstractComputing.display();
    System.out.println("cart total");

    abstractComputing = new BuyTotalComputing();
    abstractComputing.display();
    System.out.println("buy total end");

    abstractComputing = new BuyNowTotalComputing();
    abstractComputing.display();
    System.out.println("buy now total end");

    abstractComputing = new BuyAndReadComputing();
    abstractComputing.display();
    System.out.println("buy and read");

    abstractComputing = new TopGoodsComputing();
    abstractComputing.display();
    System.out.println("top goods end");

    abstractComputing = new NowUserComputing();
    abstractComputing.display();
    System.out.println("now user end");

    abstractComputing = new TotalComputing();
    abstractComputing.display();
    System.out.println("total end");

    abstractComputing = new AverageVisitComputing();
    abstractComputing.display();
    System.out.println("average end");

    abstractComputing = new TopBuyComputing();
    abstractComputing.display();
    System.out.println("top buy end");

    Utils.convertData();
  }
}
