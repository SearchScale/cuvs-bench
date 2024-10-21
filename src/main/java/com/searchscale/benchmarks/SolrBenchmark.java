package com.searchscale.benchmarks;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
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
      CSV csv =  new CSV(br);

      long start = System.currentTimeMillis();
      List<SolrInputDocument> docs = new ArrayList();
      int counter = 0;



      String[] row = null;
      while((row = csv.readNext()) != null) {
         ++counter;
         if(row.length<4) {
            continue;
         }
         try {
            JSONArray vectorJson = new JSONArray(row[3]);
         } catch (JSONException e) {
            System.out.println("Invalid json at line "+ counter+ csv.line);
            break;
         }

//         if(true) continue;
         if (counter % batchSize == 0) {
            System.out.println(counter + ": " + row[0]+" "+row[1]+ " "+row[2] );
            if (index) {
               solr.add(docs);
               solr.commit();
            }

            System.out.println("Committed.");
            docs.clear();
         }



         SolrInputDocument doc = new SolrInputDocument();
         doc.addField("id", row[0]);
         JSONArray vectorJson = new JSONArray(row[3]);
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

   public static class CSV {
      String[] headers ;
      final BufferedReader rdr;
      String line;


      public CSV(Reader rdr) throws IOException {

         this.rdr = rdr  instanceof BufferedReader?
                 (BufferedReader) rdr :
                 new BufferedReader(rdr);
         String line = this.rdr.readLine();
         if(line == null)
            throw new RuntimeException("Empty or invalid CSV file.");
         headers = parseLine(line);
      }

      // Method to parse a single line of CSV, handling quoted fields
      private static String[] parseLine(String line) {
//         System.out.println(line);
         List<String> values = new ArrayList<>();
         StringBuilder currentValue = new StringBuilder();
         boolean inQuotes = false;
         char[] chars = line.toCharArray();

         for (int i = 0; i < chars.length; i++) {
            char currentChar = chars[i];

            if (currentChar == '"') {
               // Toggle the inQuotes flag
               inQuotes = !inQuotes;
            } else if (currentChar == ',' && !inQuotes) {
               // If a comma is found and we're not inside quotes, end the current value
               values.add(currentValue.toString());
               currentValue = new StringBuilder();
            } else {
               // Add the current character to the current value
               currentValue.append(currentChar);
            }
         }

         // Add the last value
         values.add(currentValue.toString());

//         System.out.println("parsed : "+ values.toString());

         return values.toArray(new String[0]);
      }

      public String[] readNext() throws IOException {
         line = this.rdr.readLine();
         if(line == null) return null;
         return parseLine(line);
      }
   }
}
    