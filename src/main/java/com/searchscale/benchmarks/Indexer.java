package com.searchscale.benchmarks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.eclipse.jetty.util.BlockingArrayQueue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.solr.common.util.JavaBinCodec.*;

public class Indexer {
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static final String EOL = "###";

    public static void indexDocs(SolrClient solrClient, long start, InputStream in, String coll, int batchSize, int threads) throws SolrServerException, IOException, InterruptedException {
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String header = br.readLine();
        AtomicLong total = new AtomicLong();
        BlockingArrayQueue<String> queue = new BlockingArrayQueue<>();
        System.out.println("\nStarting index %%%%%%%%%%%%%%%");
        Thread[] t = new Thread[threads];
        for (int i = 0; i < t.length; i++) {
             t[i] = new Thread(new IndexRunnable(i,total, queue, solrClient, coll, batchSize));
             t[i].start();
        }

        while (true) {
            String line = br.readLine();
            if(line == null) {
                queue.offer(EOL);
                break;
            }
            queue.offer(line);

        }
        for (Thread thread : t) {
            thread.join();
        }

        long end = System.currentTimeMillis();
        System.out.println("\nTotal time: " + (end - start) / 1000);
    }

    static class IndexRunnable implements Runnable{
        final AtomicLong total ;
        final BlockingArrayQueue<String> queue;
        final SolrClient solrClient;
        final String coll;
        final int batchSz;
        boolean eol =  false;
        int id;


        IndexRunnable(int id, AtomicLong total, BlockingArrayQueue<String> rows, SolrClient solrClient, String coll, int batchSz) {
            this.id = id;
            this.total = total;
            this.queue = rows;
            this.solrClient = solrClient;
            this.coll = coll;
            this.batchSz = batchSz;
        }
        private void streamDocsBatch(OutputStream os) throws IOException {
            JavaBinCodec codec = new JavaBinCodec(os, FLOAT_ARR_RESOLVER);
            codec.writeTag(ITERATOR);
            for(;;){

                String line = queue.remove();
                if(line == EOL){
                    eol = true;
                    queue.offer(line);//put it back so that other threads can exit too
                    break;
                } else {
                    MapWriter d = null;
                    try {
                        d = parse(parseLine(line));
                    } catch (Exception e) {
                        //invalid doc
                        continue;
                    }
                    try {
                        codec.writeMap(d);
                        long count = total.incrementAndGet();
                        if(count % batchSz == 0) {
                            System.out.println(count);
                            break;
                        } else if(count % 1000 ==0){
                            System.out.print(".");
                        }
                    } catch (IOException e) {
                        //error writing to server, exit
                        eol = true;
                    }
                }
            }
            codec.writeTag(END);
            codec.close();
        }


        @Override
        public void run() {
            for (; ; ) {
                if (eol) break;
                System.out.println("starting thread : "+id);
                GenericSolrRequest gsr = new GenericSolrRequest(SolrRequest.METHOD.POST, "/update",
                        new MapSolrParams(Map.of("commit", "true")))
                        .setContentWriter(new RequestWriter.ContentWriter() {
                            @Override
                            public void write(OutputStream os) throws IOException {
                                streamDocsBatch(os);
                            }


                            @Override
                            public String getContentType() {
                                return CommonParams.JAVABIN_MIME;
                            }

                        });
                try {
                    gsr.process(solrClient, coll);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }


    static MapWriter parse(String[] row) {
        String id;
        String title;
        String article;
        float[] article_vector;
        if (row.length < 4) {
            throw new IllegalArgumentException("Invalid row");
        }

        id = row[0];
        title = row[1];
        article = row[2];
        try {
            String json = row[3];
            if (json.charAt(0) != '[') {
                throw new IllegalArgumentException("Invalid json");
            }

            List<Float> floatList = OBJECT_MAPPER.readValue(json, valueTypeRef);
            article_vector = new float[floatList.size()];
            for (int i = 0; i < article_vector.length; i++) {
                article_vector[i] = floatList.get(i);
            }

            return ew -> {
                ew.put("id", id);
                ew.put("title", title);
                ew.put("article", article);
                ew.put("article_vector", article_vector);

            };


        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON");

        }

    }
    private static String[] parseLine(String line) {
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
        return values.toArray(new String[0]);
    }
    static TypeReference<List<Float>> valueTypeRef = new TypeReference<>() {
    };
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
}