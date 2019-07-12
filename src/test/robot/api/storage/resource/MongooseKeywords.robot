*** Settings ***
Resource    common.robot

*** Keywords ***
Execute Mongoose Scenario
    [Timeout]    5 minutes
    [Arguments]    ${args}
    ${host_working_dir} =    OperatingSystem.Get Environment Variable    HOST_WORKING_DIR
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
    OperatingSystem.Run    docker stop ${MONGOOSE_CONTAINER_NAME}
    OperatingSystem.Run    docker rm ${MONGOOSE_CONTAINER_NAME}

Validate Metrics Total Log File
    [Arguments]    ${step_id}    ${op_type}    ${count_succ}    ${count_fail}    ${transfer_size}
    @{metricsTotal} =    Read CSV File To Associative    ${LOG_DIR}/${step_id}/metrics.total.csv
    BuiltIn.Should Be Equal As Strings    &{metricsTotal[0]}[OpType]    ${op_type}
    BuiltIn.Should Be Equal As Strings    &{metricsTotal[0]}[CountSucc]    ${count_succ}
    BuiltIn.Should Be Equal As Strings    &{metricsTotal[0]}[CountFail]    ${count_fail}
    BuiltIn.Should Be Equal As Strings    &{metricsTotal[0]}[Size]    ${transfer_size}