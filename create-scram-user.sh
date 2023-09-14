#!/bin/sh

if [ "$#" -ne 2 ]; then
  echo "Usage: create-scram-user <username> <password>"
  exit 1
fi

export USERNAME=$1
export PASSWORD=$2
export BOOTSTRAP=localhost:9094
export CLUSTER_ID=
export TOPIC=proactive-healthcheck
export GROUP=proactive-HC-group

echo "Create user=$USERNAME, password=$PASSWORD"
kafka-configs --bootstrap-server $BOOTSTRAP --command-config client.properties --alter --add-config "SCRAM-SHA-512=[password=$PASSWORD]" --entity-type users --entity-name $USERNAME || echo "Create failed"

confluent login --url http://localhost:8090

echo "Assign DeveloperWrite role to $USERNAME"
confluent iam rbac role-binding create --principal User:$USERNAME --role DeveloperWrite --resource Topic:${TOPIC} --kafka-cluster $CLUSTER_ID

echo "Assign DeveloperRead role to $USERNAME"
confluent iam rbac role-binding create --principal User:$USERNAME --role DeveloperRead --resource Topic:${TOPIC} --kafka-cluster $CLUSTER_ID

echo "Assign DeveloperRead role to consumer group $GROUP"
confluent iam rbac role-binding create --principal User:$USERNAME --role DeveloperRead --resource Group:${GROUP} --kafka-cluster $CLUSTER_ID
