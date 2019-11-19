package com.huobi.client.examples;

import com.huobi.client.SubscriptionClient;
import com.huobi.client.model.enums.CandlestickInterval;

import java.time.Instant;
import java.util.Date;

public class SubscribeCandlestickEvent {
    public static void main(String[] args) {
        SubscriptionClient subscriptionClient = SubscriptionClient.create();
        String symbol = "btcusdt";
//    subscriptionClient.subscribeCandlestickEvent(symbol, CandlestickInterval.MIN15, (candlestickEvent) -> {
//      System.out.println("--------------- Subscribe Candlestick ------------------");
//      System.out.println("id: " + candlestickEvent.getData().getId());
//      System.out.println("Timestamp: " + candlestickEvent.getData().getTimestamp());
//      System.out.println("High: " + candlestickEvent.getData().getHigh());
//      System.out.println("Low: " + candlestickEvent.getData().getLow());
//      System.out.println("Open: " + candlestickEvent.getData().getOpen());
//      System.out.println("Close: " + candlestickEvent.getData().getClose());
//      System.out.println("Volume: " + candlestickEvent.getData().getVolume());
//    });

        subscriptionClient.requestCandlestickEvent(symbol, 1541865660L, 1541865780L, CandlestickInterval.MIN1, (candlestickEvent) -> {
            System.out.println("--------------- Request Candlestick ------------------");
            System.out.println("===size:" + candlestickEvent.getData().size() + "===");
            candlestickEvent.getData().forEach(candlestick -> {
                System.out.println("id: " + new Date(candlestick.getId() * 1000));
                System.out.println("Timestamp: " + candlestick.getTimestamp());
                System.out.println("High: " + candlestick.getHigh());
                System.out.println("Low: " + candlestick.getLow());
                System.out.println("Open: " + candlestick.getOpen());
                System.out.println("Close: " + candlestick.getClose());
                System.out.println("Volume: " + candlestick.getVolume());
                System.out.println("--------------------------------------------");
            });
        });
    }
}
