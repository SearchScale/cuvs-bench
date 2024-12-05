package com.searchscale.benchmarks;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class SolrBenchmark {
    public static void main(String[] args) throws  IOException, SolrServerException, InterruptedException {
        Params p = new Params(args);


       //example data_file=a.csv.zip batch_size=50000 threads=1 query=true query_file=questions.vec.txt.gz

        SolrClient client = new Http2SolrClient.Builder(p.solrUrl)
                .build();
        try (InputStream in = new GZIPInputStream(new FileInputStream(p.dataFile))) {
            Indexer.indexDocs(client, System.currentTimeMillis(), in,  p.testColl, p.batchSize,p.threads);
        }


        if(p.runQuery){
            Searcher.query(client,p);
        }
        client.close();

    }

    public static Map<String, String> parseStringToMap(String[] pairs) {
        Map<String, String> map = new HashMap<>();

        // Split the input string by commas to get individual key-value pairs

        for (String pair : pairs) {
            // Split each pair by '=' to separate key and value
            String[] keyValue = pair.split("=");

            // Check if the split resulted in exactly two parts
            if (keyValue.length == 2) {
                String key = keyValue[0].trim(); // Remove any potential whitespace
                String value = keyValue[1].trim();
                map.put(key, value);
            } else {
                // Log or handle malformed key-value pairs
                System.err.println("Malformed key-value pair: " + pair);
            }
        }

        return map;
    }

    public static class Params  {

        public final int threads ;

        public final String solrUrl ;
        public final String dataFile;
        public final String queryFile;
        public final String testColl ;
        public final int batchSize ;
        public final int queryCount;
        public final boolean runQuery ;
        public final String outputFile;

        public final int docsCount;
        public final boolean isLegacy;

        Map<String, String> p ;

        public Params(String[] s) {
            p =  parseStringToMap(s);
            threads = Integer.parseInt(p.getOrDefault("index_threads", "1"));
            solrUrl = p.getOrDefault("solr_url", "http://localhost:8983/solr");
            dataFile = p.get("data_file");
            testColl = p.getOrDefault("test_coll", "test");
            batchSize  = Integer.parseInt(p.getOrDefault("batch_size", "1000"));
            docsCount = Integer.parseInt(p.getOrDefault("docs_count", "10000"));
            queryFile  = p.get("query_file");
            outputFile  = p.get("output_file");

            queryCount = Integer.parseInt( p.getOrDefault("query_count","1"));
            runQuery = Boolean.parseBoolean(p.getOrDefault("query", "false"));
            isLegacy = Boolean.parseBoolean(p.getOrDefault("legacy", "false"));
        }

    }

}
    