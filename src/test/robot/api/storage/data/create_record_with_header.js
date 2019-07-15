var customKafkaHeadersConfig = {
    "storage" : {
        "driver" : {
            "record" : {
                "headers" : {
                    "testKey" : "testValues"
                }
            }
        }
    }
};

Load
    .config(customKafkaHeadersConfig)
    .run();