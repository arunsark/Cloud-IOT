mvn exec:java -Dexec.mainClass="com.manning.lp.cloud.iot.endtoend.CloudiotPubsubExampleMqttDevice" -Dexec.args="-project_id=$1 -mqtt_bridge_port=$2 -registry_id=CloudIOT_Sync -device_id=Serene_Sensor -private_key_file=rsa_private_pkcs8 -algorithm=RS256"
