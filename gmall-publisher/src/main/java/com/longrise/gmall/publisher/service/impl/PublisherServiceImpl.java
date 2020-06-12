package com.longrise.gmall.publisher.service.impl;

import com.longrise.gmall.common.constant.GmallConstant;
import com.longrise.gmall.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        /*
        String query="{\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"filter\": {\n" +
            "        \"term\": {\n" +
            "          \"logDate\": \"2020-06-05\"\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";*/

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate", date));
        searchSourceBuilder.query(boolQueryBuilder);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_NAME).addType("_doc").build();
        Integer total = 0;
        try {
            SearchResult searchResult = jestClient.execute(search);
            total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return total;
    }


    /**
     *
     * #  统计当天的总额
     * GET gmall_new_order/_search
     * {
     *   "query": {
     *     "bool": {
     *       "filter": {
     *         "term": {
     *           "createDate": "2020-06-11"
     *         }
     *       }
     *     }
     *   },
     *   "aggs": {
     *     "sum_totalmount": {
     *       "sum": {
     *         "field": "totalAmount"
     *       }
     *     }
     *   }
     * }
     *
     * @param date
     * @return
     */
    @Override
    public Double getOrderAmount(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate", date));
        searchSourceBuilder.query(boolQueryBuilder);
        // 聚合
        SumBuilder aggsBuilder = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        searchSourceBuilder.aggregation(aggsBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType("_doc").build();

        Double sum_totalamount = 0D;
        try {
            SearchResult searchResult = jestClient.execute(search);
            sum_totalamount = searchResult.getAggregations().getSumAggregation("sum_totalamount").getSum();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return sum_totalamount;
    }


    /**
     * #  统计当天的分时总额
     * GET gmall_new_order/_search
     * {
     *   "query": {
     *     "bool": {
     *       "filter": {
     *         "term": {
     *           "createDate": "2020-06-11"
     *         }
     *       }
     *     }
     *   },
     *   "aggs": {
     *     "groupby_createHour": {
     *       "terms": {
     *         "field": "createHour",
     *         "size": 24
     *       },
     *       "aggs": {
     *         "sum_totalamount": {
     *           "sum": {
     *             "field": "totalAmount"
     *           }
     *         }
     *       }
     *     }
     *   }
     *
     * }
     *
     * @param date
     * @return
     */
    @Override
    public Map getOrderAmountHourMap(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate", date));
        searchSourceBuilder.query(boolQueryBuilder);
        // 聚合
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_createHour").field("createHour").size(24);
        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        // 子聚合
        termsBuilder.subAggregation(sumBuilder);
        searchSourceBuilder.aggregation(termsBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType("_doc").build();
        Map<String, Double> hashMap = new HashMap<>();

        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                Double hourAmount = bucket.getSumAggregation("sum_totalamount").getSum();
                String hourKey = bucket.getKey();
                hashMap.put(hourKey, hourAmount);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hashMap;
    }

    @Override
    public Map getDateHourMap(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();;
        // 过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate", date));
        searchSourceBuilder.query(boolQueryBuilder);
        // 聚合
        TermsBuilder aggsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        searchSourceBuilder.aggregation(aggsBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_NAME).addType("_doc").build();

        HashMap dauHourMap = new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                String key = bucket.getKey();
                Long count = bucket.getCount();
                dauHourMap.put(key, count);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dauHourMap;
    }


}
