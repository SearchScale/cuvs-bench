package com.searchscale.benchmarks;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.apache.solr.common.util.JavaBinCodec.*;

public class SolrBenchmark {
    public static void main(String[] args) throws  IOException, SolrServerException, InterruptedException {
        String filename = args[0];
        boolean index = Boolean.parseBoolean(args[1]);
        int totalDocs = Integer.parseInt(args[2]);
        int batchSize = Integer.parseInt(args[3]);
        String testColl = "test";
        HttpSolrClient client = (new Builder()).withBaseSolrUrl("http://localhost:8983/solr").build();


        try (InputStream in = new GZIPInputStream(new FileInputStream(new File(filename)))) {
            indexDocs(client, 0, in,  testColl, batchSize);
        }

    }

    private static void indexDocs(SolrClient solrClient, long start, InputStream in, String coll, int batchSize) throws SolrServerException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        System.out.println("Starting index:");
        CSV csv = new CSV(br);
        for (; ; ) {
            String[] row = csv.readNext();
            if (row == null) break;
            indexBatch(solrClient, csv, row, batchSize, coll);
            System.out.print(".");
        }
        long end = System.currentTimeMillis();
        System.out.println("\nTotal time: " + (double) (end - start) / 1000.0D);
    }

    private static void indexBatch(SolrClient solrClient, CSV csv, String[] firstRow, int count, String coll) throws SolrServerException, IOException {
        GenericSolrRequest gsr = new GenericSolrRequest(SolrRequest.METHOD.POST, "/update",
                new MapSolrParams(Map.of("commit", "true")))
                .setContentWriter(new RequestWriter.ContentWriter() {
                    @Override
                    public void write(OutputStream os) throws IOException {
                        int counter = 0;
                        JavaBinCodec codec = new JavaBinCodec(os, FLOAT_ARR_RESOLVER);
                        codec.writeTag(ITERATOR);
                        String[] row = firstRow;
                        for (; ; ) {
                            if (row == null) break;
                            Doc d = new Doc(row);
                            if (d.isErr) continue;
                            codec.writeMap(d);
                            ++counter;
                            if (counter > count) break;
                            row = csv.readNext();
                        }
                        codec.writeTag(END);
                        codec.close();
                    }

                    @Override
                    public String getContentType() {
                        return CommonParams.JAVABIN_MIME;
                    }
                });
        gsr.process(solrClient, "test");
    }

    static JavaBinCodec.ObjectResolver FLOAT_ARR_RESOLVER = (o, c) -> {
        if (o instanceof float[]) {
            c.writeTag(ARR, ((float[]) o).length);
            for (float v : (float[]) o) {
                c.writeFloat(v);
            }
            return null;
        } else {
            return o;
        }
    };

    static class Doc implements MapWriter {
        String id;
        String title;
        String article;
        float[] article_vector;
        boolean isErr = false;

        public Doc() {
        }


        public Doc(String[] row) {
            if (row.length < 4) {
                //invalid row
                isErr = true;
                return;
            }
            this.id = row[0];
            this.title = row[1];
            this.article = row[2];
            try {
                List<Object> vectorJson = (List<Object>) Utils.fromJSONString(row[3]);
                article_vector = new float[vectorJson.size()];
                for (int i = 0; i < vectorJson.size(); ++i) {
                    article_vector[i] = ((Number) vectorJson.get(i)).floatValue();
                }
            } catch (Exception e) {
                isErr = true;
            }
        }

        @Override
        public void writeMap(EntryWriter ew) throws IOException {
            ew.put("id", id);
            ew.put("title", title);
            ew.put("article", article);
            ew.put("article_vector", article_vector);
        }
    }

    public static class CSV {
        String[] headers;
        final BufferedReader rdr;
        String line;
        boolean eof = false;


        public CSV(Reader rdr) throws IOException {

            this.rdr = rdr instanceof BufferedReader ?
                    (BufferedReader) rdr :
                    new BufferedReader(rdr);
            String line = this.rdr.readLine();
            if (line == null)
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
            if (eof) return null;
            line = this.rdr.readLine();
            if (line == null) {
                eof = true;
                return null;
            }
            String[] strings = parseLine(line);

            return strings;
        }
    }

}
    