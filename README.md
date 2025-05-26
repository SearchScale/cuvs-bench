Prerequisites
=============

* Install CUDA (12.6 or 12.8)
* Install libnccl-dev
* Install JDK 22
* Install Maven 3.9.6+

Building cuVS 25.02
===================

* Clone https://github.com/rapidsai/cuvs
* Checkout branch branch-25.02
* ./build.sh libcuvs
* ./build.sh java

(This should install the Java artifacts to the local Maven repository)


Building Lucene with cuVS
=========================

* Clone https://github.com/searchscale/lucene
* Checkout branch `cuvs-integration-10x`
* ./gradlew mavenToLocal

(This will build Lucene 10.2.0-SNAPSHOT and install artifacts to local Maven repository)

Building Solr with Lucene and cuVS
==================================

* Clone https://github.com/searchscale/solr
* Checkout branch `ishan/cuvs-integration`
* ./gradlew assemble distTar

(This will build Solr 10.0.0-SNAPSHOT and place artifacts in ./solr/packaging/build/distributions/solr-10.0.0-SNAPSHOT.tgz)

Building benchmarking project
=============================

* Clone https://github.com/searchscale/cuvs-bench
* Checkout branch `noble/cuvs-panama`
* `mvn compile assembly:single`

(This will create the artifact ./target/solr-cuvs-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar)

Preparing Dataset
=================

* Create a new work folder, say `/home/ishan/workingarea`.
* Copy the following into the `workingarea`:
	- cp code/solr/solr/packaging/build/distributions/solr-10.0.0-SNAPSHOT.tgz workingarea/.
	- cp -r code/solr/solr/example/cuvsexample workingarea/.
	- cp -r code/cuvs-bench/upload_all.sh workingarea/.
	- cp -r code/cuvs-bench/start-solr10.sh workingarea/.
* Download the dataset:
	- wget https://accounts.searchscale.com/datasets/wikipedia/wiki_dump_5Mx2048D.csv.gz
* Create batches of Javabin payloads
	- mkdir batches
	- java -cp solr-cuvs-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar com.searchscale.benchmarks.Indexer data_file=wiki_dump_5Mx2048D.csv.gz output_file=batches/wiki batch_size=50000 docs_count=200000 legacy=true
	
	(This will create 4 batches of 50k each, total of 200k documents, in batches folder)

Running Solr & Uploading batches
================================

chmod +x *.sh
./start-solr10.sh
./upload_all.sh

Visit http://localhost:8983 in browser, click "test" on left pane under "Collections", and issue a query "*:*". It should show numFound as 200k
