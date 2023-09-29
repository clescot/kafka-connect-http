/bin/bash
id
file_path="/tmp/clusterID/clusterID"

if [ ! -f "$file_path" ]; then
  /bin/kafka-storage random-uuid > $file_path
  echo "Cluster id has been  created..."
fi