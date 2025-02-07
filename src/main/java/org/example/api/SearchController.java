package org.example.api;

import org.json.simple.JSONObject;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@RestController
public class SearchController {
    @GetMapping("/search")
    public JSONObject searchIndex(@RequestParam String term){
        List<String> cities = Arrays.asList("İstanbul","Ankara","Giresun","Sakarya","Zonguldak","Sivas","Trabzon","Yozgat");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Random random = new Random();
        int i = random.nextInt(cities.size());

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("search",term);
        jsonObject.put("time",timestamp);
        jsonObject.put("region",cities.get(i));

        System.out.println(jsonObject.toJSONString());
        return jsonObject;
    }
}
