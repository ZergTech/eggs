package com.zerg.egg.utils;

import com.huobi.client.SubscriptionClient;
import com.huobi.client.model.enums.CandlestickInterval;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author wangpengcai
 * @Date 2019-11-19 14:44
 * @Description TODO
 **/
public class OneYearDateStore {
    private static MongoDatabase huobi_db = MongoDBUtils.getConnect("47.98.45.40", "huobi_db");


    public static void saveData(String symbol, long from, long to){
        MongoCollection<Document> btc = huobi_db.getCollection("BTC");
        SubscriptionClient subscriptionClient = SubscriptionClient.create();
        subscriptionClient.requestCandlestickEvent(symbol, from, to, CandlestickInterval.MIN1, (candlestickEvent) -> {
            System.out.println("--------------- Request Candlestick ------------------");
            System.out.println("===size:" + candlestickEvent.getData().size() + "===");
            Document doc = new Document();
            candlestickEvent.getData().forEach(candlestick -> {
                FindIterable<Document> docs = btc.find(Filters.eq("_id", candlestick.getId().toString()));
                if(!docs.iterator().hasNext()){
                    DateFormat df= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    doc.append("_id",candlestick.getId().toString());
                    doc.append("Date",df.format(new Date(candlestick.getId() * 1000)));
                    doc.append("amount",candlestick.getAmount().toString());
                    doc.append("count",String.valueOf(candlestick.getCount()));
                    doc.append("open",candlestick.getOpen().toString());
                    doc.append("close",candlestick.getClose().toString());
                    doc.append("low",candlestick.getLow().toString());
                    doc.append("high",candlestick.getHigh().toString());
                    doc.append("vol",candlestick.getVolume().toString());
                    doc.append("Timestamp",String.valueOf(candlestick.getTimestamp()));
                    btc.insertOne(doc);
                    System.out.println(doc.get("Date"));
                }else{
                    System.out.println("记录存在！！！");
                }
            });
        });
    }
    public static void sendData(String symbol, long from, long to){
        while(from < to){
            long from_tmp = from + 60 * 60 + 1;
            if (from_tmp >= to){
                from_tmp = to;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            saveData(symbol, from, from_tmp);
            from = from_tmp ;
        }
    }
    //输入时间范围 进行入库
    public static void main(String[] args) {
        // 2017.11.01 1509465600
        // 2018.01.01 1514736000
        // 2018.02.01 1517414400
        // 2018.03.01 1519833600
        // 2018.04.01 1522512000
        // 2018.05.01 1525104000
        // 2018.07.01 1530374400
        // 2018.10.01 1538323200
        // 2019.01.01 1546272000
        String symbol = "btcusdt";
        long start = System.currentTimeMillis();
        sendData(symbol, 1517414400L,1519833600L);
        long end = System.currentTimeMillis();
        System.out.println("cost time:" + (end - start) + "ms");

    }


}
