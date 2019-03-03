ARG MONGOOSE_VERSION

FROM emcmongoose/mongoose:${MONGOOSE_VERSION}

ARG MONGOOSE_VERSION

ADD build/libs/mongoose-storage-driver-kafka.jar /opt/mongoose/mongoose-storage-driver-kafka.jar
ADD docker/entrypoint-storage-driver-kafka.sh /opt/mongoose/entrypoint-storage-driver-kafka.sh
ADD docker/entrypoint-storage-driver-kafka-debug.sh /opt/mongoose/entrypoint-storage-driver-kafka-debug.sh

RUN mkdir $HOME/.mongoose/${MONGOOSE_VERSION}; \
    mkdir $HOME/.mongoose/${MONGOOSE_VERSION}/ext; \
    ln -s /opt/mongoose/mongoose-storage-driver-kafka.jar $HOME/.mongoose/${MONGOOSE_VERSION}/ext/mongoose-storage-driver-kafka.jar; \
    chmod +x /opt/mongoose/entrypoint-storage-driver-kafka.sh

ENTRYPOINT ["/opt/mongoose/entrypoint-storage-driver-kafka.sh"]
