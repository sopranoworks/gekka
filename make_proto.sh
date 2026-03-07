#!/bin/sh
#SRC_DIR="pekko/remote/src/main/protobuf"
SRC_DIR="pekko/cluster/src/main/protobuf"
PACKAGE="/gekka"
# ArteryControlFormats.proto	
# ContainerFormats.proto		
# SystemMessageFormats.proto	
#WireFormats.proto
#protoc -I=${SRC_DIR} --go_out=. \
#  --go_opt=MArteryControlFormats.proto=${PACKAGE} \
#  --go_opt=MContainerFormats.proto=${PACKAGE} \
#  --go_opt=MSystemMessageFormats.proto=${PACKAGE} \
#  --go_opt=MWireFormats.proto=${PACKAGE} \
#  ${SRC_DIR}/*.proto

protoc -I=${SRC_DIR} --go_out=. \
  --go_opt=MClusterMessages.proto=${PACKAGE} \
  ${SRC_DIR}/*.proto
