package com.searchscale.benchmarks;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.params.MapSolrParams;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class Searcher {
    public static void main(String[] args) throws IOException, SolrServerException {
        SolrBenchmark.Params p = new SolrBenchmark.Params(args);
        System.out.println(p.p.toString());
        try(SolrClient client = new Http2SolrClient.Builder(p.solrUrl).build()){
            query(client,p);
        }
    }

    public static void query(SolrClient client, SolrBenchmark.Params p) throws IOException, SolrServerException {
        long start = System.currentTimeMillis();
        try (InputStream in = new GZIPInputStream(new FileInputStream(p.queryFile))) {
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            for(int i=0;i<p.queryCount;i++) {
                String qvec = br.readLine();
                //?q={!cuvs f=vector topK=32 cagraITopK=1 cagraSearchWidth=5 }[1.0, 2.0, 3.0, 4.0]
                client.query(new MapSolrParams(Map.of("q", "{!cuvs f=article_vector topK=32 cagraITopK=1 cagraSearchWidth=5}" + qvec)));
            }
        }
        System.out.println("time taken: "+(System.currentTimeMillis() -start));
    }
}
