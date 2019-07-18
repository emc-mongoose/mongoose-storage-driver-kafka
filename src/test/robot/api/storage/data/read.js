PreconditionLoad
    .config({
        "item" : {
	    "data" : {
                 "size" : 10
	     },	
            "type" : "data",
            "output" : {
                "path" : "test3"
            }
        },
         "load" : {
            "op" : {
		"limit" : {"count" : 10},
            }
        }
       
    })
    .run();

ReadLoad
    .config({
        "item" : {
            "type" : "data",
            "input" : {
                "path" : "test3"
            }
        },
        "load" : {
            "op" : {
                "type" : "read",
                "recycle" : true,
		"limit" : {"count" : 10}
            }
        }
    })
    .run();

