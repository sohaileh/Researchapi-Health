const express = require("express");
const bodyParser = require("body-parser");
const Request = require("request");
const fs = require('fs');
var AWS = require("aws-sdk");

var AWSDynamo = require("aws-sdk");
var awsurlss = [];
//----------------------------------------------------------connection to s3---------------------------------------------------------
var awsConfig = {
    "region": "us-east-2",
    "endpoint": "http://s3.us-east-2.amazonaws.com",
    
};
//----------------------------------------------------------connection to DynamoDb---------------------------------------------------------
let awsConfigDynamo = {
    "region": "us-east-2",
    "endpoint": "http://dynamodb.us-east-2.amazonaws.com",
};

AWS.config.update(awsConfigDynamo);
var docClient = new AWS.DynamoDB.DocumentClient();
//making an express object
const app = express();
//server listen
app.listen(3000, function (req, res) {
});
console.log("server is listening at port 3000");

//body parser code middle wares
app.use(bodyParser.urlencoded({
    extended: false
}));
app.use(bodyParser.json());
//set cros header
app.use(function (req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});
//------------------------------------------------HealthApi---------------------------------------------------------------
////using get route
app.get('/Researchapi/Health', function (req, res) {
    //console.log(req.query["keyword"]);

    FetchFromFile(res, req.query);
})


//fething function
function FetchFromFile(res, query) {

    var search = query["Searchkey"];
    var modifieddateT = query["modifieddate"];
    var allDataset = [];
    fs.readFile('./fulldata.json', 'utf8', function (err, data) {
        if (err) throw err;
        allData = JSON.parse(data);

        var rows = allData.dataset;
        var issearched = false;
        for (var index = 0; index < rows.length; index++) {
            var row = rows[index];
            if (search != "" && search != undefined) {
                for (var i = 0; i < row.keyword.length; i++) {
                    if ( row.keyword[i].toLowerCase().indexOf(search.toLowerCase()) !== -1 ) {
                        issearched = true;
                    }
                }
                if ( row.title.toLowerCase().indexOf(search.toLowerCase()) !== -1  || row.modified == modifieddateT ) {
                    issearched = true;
                }
                if (issearched)
                {
                    row.s3url="No";
                    allDataset.push(row);
                    
                issearched = false;
                }
                
            }
            else {
                allDataset.push(row);
            }
        }
        res.status(200).send(allDataset);
    });
}
//---------------------------------------------------------Fetch From Database--------------------------------------------------------------------
function FetchFromDatabase(res, query) {
    var search = query["Searchkey"];
    var allDataset = [];
    var issearched = false;
    var params;
    if (search == undefined || search == "") {
        params = {
            TableName: "HealthDataCatalog",

        };
    }
    else {
        params = {
            TableName: "HealthDataCatalog",
            FilterExpression: "(contains(#K, :keyword)) or  (contains(#f, :title)) ",
            ExpressionAttributeNames: {
                "#f": "title",
                "#K": "keyword"
            },
            ExpressionAttributeValues: {
                ":title": search,
                ":keyword": search
            }
        };
    }
    docClient.scan(params, onScan);
    function onScan(err, data) {
        if (err) {
            console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            data.Items.forEach(function (item) {
                allDataset.push(item);
            });
        }
        res.status(200).send(allDataset);
    }
}

//--------------------------------------------------------------My Research Api----------------------------------------------------------------
//using get route
app.get('/myresearch', function (req, res) {
    FetchFroms3link(res, req.query["userName"]);
});
//---------------------------------------------------------Fetch From Database--------------------------------------------------------------------
function FetchFroms3link(res, search) {
    console.log(search)
    var allDataset = [];
    var issearched = false;
    var params;
        var params = {
            TableName: "s3links",
            FilterExpression: "#K=:created_by",
            ExpressionAttributeNames: {
                "#K": "created_by"
            },
            ExpressionAttributeValues: {
                ":created_by":  search
            }
        };
        docClient.scan(params, onScan);
        function onScan(err, data) {
            if (err) {
                console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
            } else {
                data.Items.forEach(function(item) {
                    allDataset.push("title:"+item.title);
                    allDataset.push("accessUrl:"+item.AccessUrl);
                    allDataset.push("createdBy:"+item.created_by);
                    allDataset.push("createdOn:"+item.created_on);
                    allDataset.push("Description:"+item.description);
                    //allDataset.push("Acces"+item.identifier);
                    allDataset.push("awsURL:"+item.s3link);
                    //console.log(item.AccessUrl);
                });
            } 
                res.status(200).send(allDataset);
        }
    }
//---------------------------------------------------------save Api-----------------------------------------------------------------------------
//using get route
app.get('/Researchapi/Health/save', function (req, res) {
    //searchindb(res, req.query["Searchkey"],req.query["userName"]);
    try {
        console.log(req.headers)
        var query = JSON.parse(req.headers["query"]);
    var userName = req.headers["username"];
        
        if (query.ids) {
            for (var k = 0; k < query.ids.length; k++) {
                var search = query.ids[k].value;
                searchindb(res, search,userName);
            }
            res.status(200).send("OK");
        }
    }
    catch (e) { res.status(500).send(e); }

})
//------------------------------------------------Getting Data From Our API------------------------------------------------
function searchindb(res, search,userName) {
    //var search = query;
    console.log(search);
    var allDataset = [];
    var issearched = false;
    var params = {
        TableName: "HealthDataCatalog",
        FilterExpression: "#K=:identifier ",
        ExpressionAttributeNames: {
            "#K": "identifier"
        },
        ExpressionAttributeValues: {
            ":identifier": search
        }
    };

    docClient.scan(params, onScan);

    function onScan(err, data) {
        console.log("Onscan");
        if (err) {
            console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            data.Items.forEach(function (item) {
                awsurlss=[];
                if (item.distribution != undefined) {
                    for (var i = 0; i < item.distribution.length; i++) {
                        getdata(item.distribution[i].accessURL, item.title + "_Distribution_" + i, item.identifier,item.description,userName,item.title)
                    }
                   // saveDynamodb(awsurlss,"Testing","fert", "sssssss","admin","Admin");
                    
                }

            });

            issearched = false;
        }
    }

}
//------------------------------------------------Extract Data from Clicked Url------------------------------------------------
function getdata(url, title, identifier,description,userName,title1) {
        datta = Request.get({
            "headers": {
                "content-type": "application/json",
                "Origin": '*'
            },
            "url": url,
        }, (error, response, body) => {
            if (error) {
                return console.log(error);
            } else {
                saveData(body, title, url, identifier,description,userName,title1);
            }
        })
    
}
//------------------------------------------------Store Data behind Url into S3--------------------------------------------
AWS.config.update(awsConfig);
var s3 = new AWS.S3();
function saveData(body, title, url, identifier,description,userName,title1) {
    console.log('Data behind Link is successfully dumped into s3');
    var myBucket = 'research.data';
    var myKey = title,
        params = { Bucket: myBucket, Key: myKey, Body: body };
    s3.putObject(params, function (err, data) {

        if (err) {
            console.log(err)
        } else {
            
            getawsurl(myKey, title, url, identifier,description,userName,title1);
        }
    });

}
//---------------------------------------Getting Url From Aws -------------------------------------------------------------
function getawsurl(myKey, title, url, identifier,description,userName,title1) {
    var params = {
        Bucket: 'research.data',
        Key: myKey,
    };
    awsurl = s3.getSignedUrl('getObject', params);
    var value = "yes";
    updatecatalog(identifier);
    awsurlss.push(awsurl);
    saveDynamodb(awsurl,title1 ,url, identifier,description,userName);


}
//------------------------------------------------Passing url to DynamoDb---------------------------------------------------
let saveDynamodb = function (awsurl, title, url, identifier,description,userName) {
    AWS.config.update(awsConfigDynamo);
    let docClient = new AWS.DynamoDB.DocumentClient();

    var input = {
        "title": title, "created_by": userName, "created_on": new Date().toString(),
        "updated_by": userName.toLowerCase(),
        "AccessUrl": url,
        "s3link": awsurl,
        "description": description,
        "identifier": identifier
    };
    var params = {
        TableName: "s3links",
        Item: input
    };
    docClient.put(params, function (err, data) {
        if (err) {
            console.log("Link  cant be added to DynamoDB" + JSON.stringify(err, null, 2));
        } else {
            console.log("Link added successfully to dynamoDB");
        }
    });
}
//----------------------------------------------update Catalog Table----------------------------------

function updatecatalog(identifier) {
    AWS.config.update(awsConfigDynamo);
    let docClient = new AWS.DynamoDB.DocumentClient();
    var params = {
        TableName: "HealthDataCatalog",
        Key: {
            "identifier": identifier
        },
        UpdateExpression: "SET s3link = :label",
        ExpressionAttributeValues: {
            ":label": "Yes",
        }
    };

    docClient.update(params, function (err, data) {
        if (err) {
            console.log("s3link column not updated" + JSON.stringify(err, null, 2));
        } else {
            console.log("s3link column successfully updated");
        }
    });
}
