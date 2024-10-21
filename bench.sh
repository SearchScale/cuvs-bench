CUVSJAR="/home/ishan/code/lucene-cuvs-internal/lucene/target/cuvs-searcher-lucene-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
DATAFILE="/home/ishan/code/wikipedia_vector_dump.csv.gz"

BASEDIR=`pwd`

pkill -9 java

#wget -c https://dlcdn.apache.org/solr/solr/9.7.0/solr-9.7.0.tgz
wget -c https://archive.apache.org/dist/solr/solr/9.6.1/solr-9.6.1.tgz
rm -rf solr-9.6.1; tar -xf solr-9.6.1.tgz
cd solr-9.6.1
rm -rf cuvsconf; mkdir cuvsconf

tee -a cuvsconf/solrconfig.xml << EOM
<?xml version="1.0" ?>
<config>
    <luceneMatchVersion>LATEST</luceneMatchVersion>
    <directoryFactory name="DirectoryFactory" class="solr.NRTCachingDirectoryFactory"/>
    <!-- codecFactory name="CodecFactory" class="com.searchscale.lucene.vectorsearch.SolrCuVSCodecFactory" />
    <queryParser name="cuvs" class="com.searchscale.lucene.vectorsearch.SolrCUVsQParserPlugin"/ -->
    <requestHandler name="/select" class="solr.SearchHandler"></requestHandler>
</config>
EOM


tee -a cuvsconf/schema.xml << EOM
<?xml version="1.0" ?>
<schema name="cuvs-schema" version="1.7">
    <fieldType name="string" class="solr.StrField" multiValued="true"/>
    <fieldType name="knn_vector" class="solr.DenseVectorField" vectorDimension="2048" similarityFunction="cosine" />
    <fieldType name="plong" class="solr.LongPointField" useDocValuesAsStored="false"/>
    <field name="id" type="string" indexed="true" stored="true" multiValued="false" required="false"/>
    <field name="title" type="string" indexed="true" stored="true" multiValued="false" required="false"/>
    <field name="article_vector" type="knn_vector" indexed="true" stored="true"/>
    <field name="article" type="string" indexed="true" stored="true"/>
    <field name="_version_" type="plong" indexed="true" stored="true" multiValued="false" />
    <uniqueKey>id</uniqueKey>
</schema>
EOM

sed '30i permission java.lang.RuntimePermission "loadLibrary.*";' server/etc/security.policy > tmp.policy
mv tmp.policy server/etc/security.policy

cp $CUVSJAR server/solr-webapp/webapp/WEB-INF/lib/.

bin/solr -c -m "16g"

server/scripts/cloud-scripts/zkcli.sh -zkhost localhost:9983 -cmd upconfig -confdir cuvsconf -confname cuvs
curl "http://localhost:8983/solr/admin/collections?action=CREATE&name=test&numShards=1&collection.configName=cuvs"

#indexing

#time python3 $BASEDIR/jsonify.py $DATAFILE 2048 50000 4

java -cp /home/ishan/code/solr-cuvs-benchmarks/target/solr-cuvs-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar com.searchscale.benchmarks.SolrBenchmark /home/ishan/code/wikipedia.jsonl.gz true 50000 5000

