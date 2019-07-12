*** Settings ***
Resource    resource/common.robot

Test Setup       Start Containers
Test Teardown    Stop Containers

Force Tags    Create record

*** Variables ***
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
    ${std_out} =    MongooseKeywords.Execute Mongoose Scenario    ${args}
    MongooseKeywords.Validate Metrics Total Log File    ${step_id}    CREATE    ${count_limit}    ${0}    ${10000}

*** Keyword ***
Start Containers
    [Return]    0

Stop Containers
    MongooseKeywords.Remove Mongoose Node