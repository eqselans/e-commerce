package org.example.api;

import org.example.MessageProducer;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@RestController
public class SearchController {

    @Autowired
    MessageProducer messageProducer;

    @GetMapping("/search")
    public void searchIndex(@RequestParam String term){
        List<String> cities = Arrays.asList(
                "İstanbul", "Ankara", "İzmir", "Bursa", "Adana", "Antalya", "Konya", "Kayseri", "Eskişehir", "Samsun",
                "Trabzon", "Diyarbakır", "Mersin", "Kocaeli", "Hatay", "Malatya", "Van", "Erzurum", "Gaziantep", "Sivas"
        );
        List<String> products = Arrays.asList(
                "klavye", "mouse", "monitör", "laptop", "masaüstü bilgisayar", "tablet", "telefon", "kulaklık"
        );
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        while (true) {
            Random random = new Random();
            int i = random.nextInt(cities.size());
            int k = random.nextInt(products.size());

            long offset = Timestamp.valueOf("2025-01-01 00:00:00").getTime();
            long end = Timestamp.valueOf("2020-01-01 00:00:00").getTime();
            long diff = end - offset + 1;
            Timestamp rand = new Timestamp(offset + (long) (Math.random() * diff));

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("search", products.get(k));
            jsonObject.put("current_ts", rand.toString());
            jsonObject.put("region", cities.get(i));
            jsonObject.put("userId",random.nextInt(15000-1000)+1000);

            System.out.println(jsonObject.toJSONString());
            messageProducer.send(jsonObject.toJSONString());
        }
    }


    @GetMapping("/search/stream")
    public void searchIndexStream(@RequestParam String term){
        List<String> cities = Arrays.asList(
                "İstanbul", "Ankara", "İzmir", "Bursa", "Adana", "Antalya", "Konya", "Kayseri", "Eskişehir", "Samsun");

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        Random random = new Random();
        int i = random.nextInt(cities.size());

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("search", term);
        jsonObject.put("current_ts", timestamp.toString());
        jsonObject.put("region", cities.get(i));
        jsonObject.put("userId",random.nextInt(3000-2000)+2000);

        System.out.println(jsonObject.toJSONString());
        messageProducer.send(jsonObject.toJSONString());

    }

}
