package com.searchscale.benchmarks;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class SolrBenchmark {
    public static void main(String[] args) throws  IOException, SolrServerException, InterruptedException {
        String filename = args[0];
        boolean index = Boolean.parseBoolean(args[1]);
        int totalDocs = Integer.parseInt(args[2]);
        int batchSize = Integer.parseInt(args[3]);
        int threads = Integer.parseInt(args[4]);
        String testColl = "test";
        SolrClient client = new Http2SolrClient.Builder("http://localhost:8983/solr")
                .build();


        try (InputStream in = new GZIPInputStream(new FileInputStream(new File(filename)))) {
            Indexer.indexDocs(client, System.currentTimeMillis(), in,  testColl, batchSize,threads);
        }
        client.close();

    }

}
    