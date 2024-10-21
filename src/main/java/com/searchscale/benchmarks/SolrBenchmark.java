package com.searchscale.benchmarks;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SolrBenchmark {
   public static void main(String[] args) throws FileNotFoundException, IOException, SolrServerException, InterruptedException {
      String filename = args[0];
      boolean index = Boolean.parseBoolean(args[1]);
      int totalDocs = Integer.parseInt(args[2]);
      int batchSize = Integer.parseInt(args[3]);
      BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(filename)))));
      new ObjectMapper();
      HttpSolrClient solr = (new Builder()).withBaseSolrUrl("http://localhost:8983/solr/test").build();
      long start = System.currentTimeMillis();
      List<SolrInputDocument> docs = new ArrayList();
      int counter = 0;

      String line;
      while((line = br.readLine()) != null) {
         ++counter;
         if (counter % batchSize == 0) {
            System.out.println(counter + ": " + line.substring(0, 120));
            if (index) {
               solr.add(docs);
               solr.commit();
            }

            System.out.println("Committed.");
            docs.clear();
         }

         JSONObject rawdoc = null;

         try {
            rawdoc = new JSONObject(line);
         } catch (JSONException var18) {
            continue;
         }

         SolrInputDocument doc = new SolrInputDocument();
         doc.addField("id", rawdoc.get("id"));
         JSONArray vectorJson = new JSONArray(rawdoc.getString("article_vector"));
         float[] vector = new float[vectorJson.length()];

         for(int i = 0; i < vectorJson.length(); ++i) {
            vector[i] = vectorJson.getFloat(i);
         }

         doc.addField("article_vector", vector);
         docs.add(doc);
         if (counter == totalDocs) {
            break;
         }
      }

      solr.close();
      long end = System.currentTimeMillis();
      System.out.println("Total time: " + (double)(end - start) / 1000.0D);
   }
}
    