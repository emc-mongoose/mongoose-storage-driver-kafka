*** Settings ***
Library          OperatingSystem
Library          CSVLibrary
Test Setup       Start Containers
Test Teardown    Stop Containers

Force Tags    Create record

*** Variables ***
${MONGOOSE_IMAGE_NAME} =    emcmongoose/mongoose-storage-driver-kafka
${MONGOOSE_CONTAINER_NAME} =    mongoose-storage-driver-kafka

${LOG_DIR} =    build/log

*** Test Cases ***
Create Record Test
    ${node_addr} =    OperatingSystem.Get Environment Variable    SERVICE_HOST    127.0.0.1
    ${step_id} =    BuiltIn.Set Variable    create_record_test
    OperatingSystem.Remove Directory    ${LOG_DIR}/${step_id}    recursive=True
    ${count_limit} =    BuiltIn.Set Variable    ${10}
    ${args} =    BuiltIn.Catenate    SEPARATOR= \\\n\t
    ...  --load-step-id=${step_id}
    ...  --load-op-limit-count=${count_limit}
    ...  --item-data-size=1000
    ...  --storage-net-node-addrs=${node_addr}
    ${std_out} =    Execute Mongoose Scenario    ${args}
    BuiltIn.Log    ${std_out}
    Validate Metrics Total Log File    ${step_id}    CREATE    ${count_limit}    ${0}    ${10000}

*** Keyword ***
Execute Mongoose Scenario
    [Timeout]    5 minutes
    [Arguments]    ${args}
    ${host_working_dir} =    OperatingSystem.Get Environment Variable    HOST_WORKING_DIR
    BuiltIn.Log    ${host_working_dir}
    ${version} =    OperatingSystem.Get Environment Variable    BASE_VERSION
    ${image_version} =    OperatingSystem.Get Environment Variable    VERSION
    ${cmd} =    BuiltIn.Catenate    SEPARATOR= \\\n\t
    ...  docker run
    ...  --name ${MONGOOSE_CONTAINER_NAME}
    ...  --network host
    ...  --volume ${host_working_dir}/${LOG_DIR}:/root/.mongoose/${version}/log
    ...  ${MONGOOSE_IMAGE_NAME}:${image_version}
    ...  ${args}
    ${std_out} =    OperatingSystem.Run    ${cmd}
    [Return]    ${std_out}

Remove Mongoose Node
    ${std_out} =    OperatingSystem.Run    docker logs ${MONGOOSE_CONTAINER_NAME}
    BuiltIn.Log    ${std_out}
    OperatingSystem.Run    docker stop ${MONGOOSE_CONTAINER_NAME}
    OperatingSystem.Run    docker rm ${MONGOOSE_CONTAINER_NAME}

Start Containers
    [Return]    0

Stop Containers
    Remove Mongoose Node

Validate Metrics Total Log File
    [Arguments]    ${step_id}    ${op_type}    ${count_succ}    ${count_fail}    ${transfer_size}
    @{metricsTotal} =    Read CSV File To Associative    ${LOG_DIR}/${step_id}/metrics.total.csv
    BuiltIn.Should Be Equal As Strings    &{metricsTotal[0]}[OpType]    ${op_type}
    BuiltIn.Should Be Equal As Strings    &{metricsTotal[0]}[CountSucc]    ${count_succ}
    BuiltIn.Should Be Equal As Strings    &{metricsTotal[0]}[CountFail]    ${count_fail}
    BuiltIn.Should Be Equal As Strings    &{metricsTotal[0]}[Size]    ${transfer_size}

