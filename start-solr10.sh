# Stop Solr if running
pkill -9 java # kill all java processes
rm -rf solr-10.0.0-SNAPSHOT

# Start a Solr instance
tar -xf solr-10.0.0-SNAPSHOT.tgz
cd solr-10.0.0-SNAPSHOT

bin/solr start -m 16G

(cd ../cuvsexample/conf && zip -r - *) | curl -X POST --header "Content-Type:application/octet-stream" --data-binary @- "http://localhost:8983/solr/admin/configs?action=UPLOAD&name=cuvs"
curl "http://localhost:8983/solr/admin/collections?action=CREATE&name=test&numShards=1&collection.configName=cuvs"

