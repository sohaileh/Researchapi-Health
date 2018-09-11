const express = require("express");
const bodyParser = require("body-parser");
const Request = require("request");
const fs = require('fs');
var AWS = require("aws-sdk");

var AWSDynamo = require("aws-sdk");

//----------------------------------------------------------connection to s3---------------------------------------------------------
//----------------------------------------------------------connection to s3---------------------------------------------------------
var awsConfig = {
    "region": "us-east-2",
    "endpoint": "http://s3.us-east-2.amazonaws.com",
    "accessKeyId": "AKIAJQDSJGVCUN4PZF2Q", "secretAccessKey": "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"
};
//----------------------------------------------------------connection to DynamoDb---------------------------------------------------------
let awsConfigDynamo = {
    "region": "us-east-2",
    "endpoint": "http://dynamodb.us-east-2.amazonaws.com",
    "accessKeyId": "AKIAJQDSJGVCUN4PZF2Q", "secretAccessKey": "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"
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


//using get route
app.get('/Researchapi/Health/save', function (req, res) {
   
    try {
        console.log(req.headers["query"]);
        var query = JSON.parse(req.headers["query"]);
        console.log(query)
        if (query.ids) {
            for (var k = 0; k < query.ids.length; k++) {
                var search = query.ids[k].value;
                searchindb(res, search);
            }
            res.status(200).send("OK");
        }
    }
    catch (e) { res.status(500).send("Failed"); }
   
})
//------------------------------------------------Getting Data From Our API------------------------------------------------
function searchindb(res, query) {
       console.log("searchindb");
    var search = query;
    console.log(search);
    var allDataset = [];
    var issearched = false;
    var params = {
        TableName: "HealthDataCatalog",
        FilterExpression: "#K=:identifier or  (contains(#f, :title)) ",
        ExpressionAttributeNames: {
            "#f": "title",
            "#K": "identifier"
        },
        ExpressionAttributeValues: {
            ":title": search,
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
                if (item.distribution != undefined) {
                    for (var i = 0; i < item.distribution.length; i++) {
                        getdata(item.distribution[i].accessURL, item.title + "_Distribution_" + i)
                    }
                }
                issearched = false;
            });
        }
    }

}


//------------------------------------------------Extract Data from Clicked Url------------------------------------------------
function getdata(url, title) {
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
            saveData(body, title, url);
        }
    })

}
//------------------------------------------------Store Data behind Url into S3--------------------------------------------
AWS.config.update(awsConfig);
var s3 = new AWS.S3();
function saveData(body, title, url) {
    var myBucket = 'research.data';
    var myKey = title,
        params = { Bucket: myBucket, Key: myKey, Body: body };
    s3.putObject(params, function (err, data) {

        if (err) {
            console.log(err)
        } else {
            console.log('Data behind Link is successfully dumped into s3');
            getawsurl(myKey, title, url);
        }
    });

}
//---------------------------------------Getting Url From Aws -------------------------------------------------------------
function getawsurl(myKey, title, url) {
    var params = {
        Bucket: 'research.data',
        Key: myKey,
    };
    var awsurl = s3.getSignedUrl('getObject', params);
    //console.log(`the url is${awsurl}`);
    saveDynamodb(awsurl, title, url);
}
//------------------------------------------------Passing url to DynamoDb---------------------------------------------------
let saveDynamodb = function (awsurl, title, url) {
    AWS.config.update(awsConfigDynamo);
    let docClient = new AWS.DynamoDB.DocumentClient();
    var input = {
        "title": title, "created_by": "Admin", "created_on": new Date().toString(),
        "updated_by": "Admin",
        "AccessUrl": url,
        "s3link": awsurl
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
                    allDataset.push(row);
                issearched = false;
            }
            else {
                allDataset.push(row);
            }
        }
        res.status(200).send(allDataset);
    });
}

}
