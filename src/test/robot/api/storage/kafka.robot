*** Settings ***
Force Tags  create_record
Library  OperatingSystem
Library  CSVLibrary
Test Setup  Start Containers
Test Teardown  Stop Containers

*** Variables ***
${DATA_DIR} =  src/test/robot/api/storage/data
${MONGOOSE_IMAGE_NAME} =  emcmongoose/mongoose-storage-driver-kafka
${MONGOOSE_CONTAINER_NAME} =  mongoose-storage-driver-kafka
${MONGOOSE_CONTAINER_DATA_DIR} =  /data
${LOG_DIR} =  build/log

*** Test Cases ***
Create_Record_Test
    [Tags]  create_record
    ${node_addr} =  Get Environment Variable  SERVICE_HOST  127.0.0.1
    ${step_id} =  Set Variable  create_record_test
    Remove Directory  ${LOG_DIR}/${step_id}  recursive=True
    ${count_limit} =  Set Variable  10
    ${args} =  Catenate  SEPARATOR= \\\n\t
    ...  --load-step-id=${step_id}
    ...  --load-op-limit-count=${count_limit}
    ...  --item-data-size=1000
    ...  --storage-net-node-addrs=${node_addr}
    ${std_out} =  Execute Mongoose Scenario  ${args}
    Log  ${std_out}
    Validate Metrics Total Log File  ${step_id}  CREATE  ${count_limit}  0  10000

Read_Records_Test
    [Tags]  read_record
    ${node_addr} =  Get Environment Variable  SERVICE_HOST  127.0.0.1
    ${step_id} =  Set Variable  read_record_test
    Remove Directory  ${LOG_DIR}/${step_id}  recursive=True
    ${args} =  Catenate  SEPARATOR= \\\n\t
    ...  --load-step-id=${step_id}
    ...  --storage-net-node-addrs=${node_addr}
    ...  --run-scenario=${MONGOOSE_CONTAINER_DATA_DIR}/read.js
    ${std_out} =  Execute Mongoose Scenario  ${args}
    Log  ${std_out}
    Validate Metrics Total Log File  ${step_id}  READ  10  0  100

*** Keyword ***
Execute Mongoose Scenario
    [Timeout]  5 minutes
    [Arguments]  ${args}
    ${host_working_dir} =  Get Environment Variable  HOST_WORKING_DIR
    Log  ${host_working_dir}
    ${version} =  Get Environment Variable  BASE_VERSION
    ${image_version} =  Get Environment Variable  VERSION
    ${cmd} =  Catenate  SEPARATOR= \\\n\t
    ...  docker run
    ...  --name ${MONGOOSE_CONTAINER_NAME}
    ...  --network host
    ...  --volume ${host_working_dir}/${LOG_DIR}:/root/.mongoose/${version}/log
    ...  --volume ${host_working_dir}/${DATA_DIR}:${MONGOOSE_CONTAINER_DATA_DIR}
    ...  ${MONGOOSE_IMAGE_NAME}:${image_version}
    ...  ${args}
    ${std_out} =  Run  ${cmd}
[Return] ${std_out}

Remove Mongoose Node
    ${std_out} =  Run  docker logs ${MONGOOSE_CONTAINER_NAME}
    Log  ${std_out}
    Run  docker stop ${MONGOOSE_CONTAINER_NAME}
    Run  docker rm ${MONGOOSE_CONTAINER_NAME}

Start Containers
    [Return]  0

Stop Containers
    Remove Mongoose Node

Validate Metrics Total Log File
    [Arguments]  ${step_id}  ${op_type}  ${count_succ}  ${count_fail}  ${transfer_size}
    @{metricsTotal} =  Read CSV File To Associative  ${LOG_DIR}/${step_id}/metrics.total.csv
    Should Be Equal As Strings  &{metricsTotal[0]}[OpType]  ${op_type}
    Should Be Equal As Strings  &{metricsTotal[0]}[CountSucc]  ${count_succ}
    Should Be Equal As Strings  &{metricsTotal[0]}[CountFail]  ${count_fail}
    Should Be Equal As Strings  &{metricsTotal[0]}[Size]  ${transfer_size}

