ARG BASE_VERSION

FROM adoptopenjdk/openjdk11:alpine
ADD src ./src/
ADD gradle ./gradle/
ADD build.gradle ./build.gradle
ADD gradlew ./gradlew
ADD settings.gradle ./settings.gradle
RUN ./gradlew clean jar

FROM emcmongoose/mongoose-base:${BASE_VERSION}
ARG BASE_VERSION
ARG LOAD_STEP_PIPELINE_VERSION
ARG STORAGE_DRIVER_PREEMPT_VERSION
ARG VERSION
RUN mkdir -p $HOME/.mongoose/${BASE_VERSION}/ext
COPY --from=0 /build/libs/mongoose-storage-driver-kafka-${VERSION}.jar /tmp/
ADD ci/docker/entrypoint_storage_driver_kafka.sh /opt/mongoose/entrypoint_storage_driver_kafka.sh
RUN mv -f /tmp/mongoose-storage-driver-kafka-*.jar $HOME/.mongoose/${BASE_VERSION}/ext/ \
    && chmod +x /opt/mongoose/entrypoint_storage_driver_kafka.sh \
    && curl http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-load-step-pipeline/${LOAD_STEP_PIPELINE_VERSION}/mongoose-load-step-pipeline-${LOAD_STEP_PIPELINE_VERSION}.jar -o $HOME/.mongoose/${BASE_VERSION}/ext/mongoose-load-step-pipeline.jar \
    && curl http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-preempt/${STORAGE_DRIVER_PREEMPT_VERSION}/mongoose-storage-driver-preempt-${STORAGE_DRIVER_PREEMPT_VERSION}.jar -o $HOME/.mongoose/${BASE_VERSION}/ext/mongoose-storage-driver-preempt.jar
ENTRYPOINT ["/opt/mongoose/entrypoint_storage_driver_kafka.sh"]
