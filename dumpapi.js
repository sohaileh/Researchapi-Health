const express = require("express");
const bodyParser = require("body-parser");
const Request = require("request");
const fs = require('fs');
var AWS = require("aws-sdk");
var multer = require('multer');
multerS3 = require('multer-s3');
const uuidv4 = require('uuid/v4');
var awsurlss = [];

var Keywords = [];
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
//=====================================================HealthApi=========================================================
////using get route
app.get('/Researchapi/Health', function (req, res) {
    console.log(req.query["Searchkey"]);
    FetchFromDatabase(res, req.query);
});
//fething function
//================================================Fetch From Database========================================================
function FetchFromDatabase(res, query) {
    var search = query["Searchkey"];
    var allDataset = [];
    Keywords = [];
    var allDatasets = [];
    var issearched = false;
    var params;
    params = {
        TableName: "HealthDataCatalog",
    };
    docClient.scan(params, onScan);
    function onScan(err, data) {
        if (err) {
            console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            var rows = data.Items;
            for (var index = 0; index < rows.length; index++) {
                var row = rows[index];
                row.KeywordCount = {};
                for (var i = 0; i < row.keyword.length; i++) {
                    Keywords.push(row.keyword[i]);
                }
            }
            var allDataset = [];
            var issearched = false;
            if (search != "" && search != undefined) {
                for (var index = 0; index < rows.length; index++) {
                    var row = rows[index];
                    for (var i = 0; i < row.keyword.length; i++) {
                        if (row.keyword[i].toLowerCase().indexOf(search.toLowerCase()) !== -1) {
                            issearched = true;
                        }
                    }
                    if (row.title.toLowerCase().indexOf(search.toLowerCase()) !== -1 || row.description.toLowerCase().indexOf(search.toLowerCase()) !== -1) {
                        issearched = true;
                    }
                    if (issearched) {
                        row.source = "healthdata.gov";
                        allDataset.push(row);
                        issearched = false;
                    }
                }
            }
            else {
                allDataset = rows;
                for (var index = 0; index < allDataset.length; index++) {
                    allDataset[index].source = "healthdata.gov";
                }
            }
            //CALL TO USER COLLABORATION TABLE
            datafromusercollaboration(search, allDataset, res)
        }
    }
    //===========================================================PRINTS DATA FROM USER COLLABORATION================================================
    function datafromusercollaboration(search, allDataset, res) {
        params = {
            TableName: "UserCollaboration",
        };
        docClient.scan(params, onScan);
        function onScan(err, data) {
            if (err) {
                console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
            } else {
                var rowss = data.Items;
                for (var index = 0; index < rowss.length; index++) {
                    var row = rowss[index];
                    row.KeywordCount = {};
                    for (var i = 0; i < row.keyword.length; i++) {
                        Keywords.push(row.keyword[i]);
                    }
                }
                var issearched = false;
                if (search != "" && search != undefined) {
                    for (var index = 0; index < rowss.length; index++) {
                        var row = rowss[index];
                        if (row.title.toLowerCase().indexOf(search.toLowerCase()) !== -1 || row.description.toLowerCase().indexOf(search.toLowerCase()) !== -1) {
                            issearched = true;
                        }
                        if (issearched) {
                            row.source = "published data";
                            allDataset.push(row);
                            issearched = false;
                        }
                    }
                }
                else {
                    for (var index = 0; index < rowss.length; index++) {

                        var row = rowss[index];
                        row.source = "Publisher";
                        allDataset.push(row);
                    }


                }
                //CALL TO CMS.GOV API
                datafromCMSGOV(search, allDataset, res)
            }

        }
    }
}
//===================================================================PRINTS DATA FROM CMS API========================================================
function datafromCMSGOV(search, allDataset, res) {
    console.log("I am in CMS GOV ##############");
    params = {
        TableName: "TestHealth",
    };
    docClient.scan(params, onScan);
    function onScan(err, data) {
        if (err) {
            console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            var rowss = data.Items;
            for (var index = 0; index < rowss.length; index++) {
                var row = rowss[index];
                row.KeywordCount = {};
                for (var i = 0; i < row.keyword.length; i++) {
                    Keywords.push(row.keyword[i]);
                }
            }
            var issearched = false;
            if (search != "" && search != undefined) {
                for (var index = 0; index < rowss.length; index++) {
                    var row = rowss[index];
                    for (var i = 0; i < row.keyword.length; i++) {
                        if (row.keyword[i].toLowerCase().indexOf(search.toLowerCase()) !== -1) {
                            issearched = true;
                        }
                    }
                    console.log(search);
                    if (row.title.toLowerCase().indexOf(search.toLowerCase()) !== -1 || row.description.toLowerCase().indexOf(search.toLowerCase()) !== -1) {
                        issearched = true;
                    }
                    if (issearched) {
                        row.source = "data.cms.gov";
                        allDataset.push(row);
                        issearched = false;
                    }
                }

            }
            else {
                for (var index = 0; index < rowss.length; index++) {
                    var row = rowss[index];
                    row.source = "data.cms.gov";

                    allDataset.push(row);
                }

            }
            if (allDataset.length > 0)
                allDataset[allDataset.length - 1].KeywordCount = countoccurences(Keywords)
            res.status(200).send(allDataset);

        }

    }
}
//===============================================================COUNT KEYWORDS IN API================================================================
function countoccurences(allDataset) {
    //var fullnames = ['sohail', 'omerr', 'sohail', 'lubaid', 'omer', 'sohail', 'omer', 'lubaid', 'lubaid', 'sohail', 'sohail'];
    var occurences = {};

    for (var i = 0; i < allDataset.length; i++) {
        if (typeof occurences[allDataset[i]] == "undefined") {
            occurences[allDataset[i]] = 1;
        } else {
            occurences[allDataset[i]]++;
        }
    }
    return sortMapByValue(occurences);
}
//SORT FUNCTION
function sortMapByValue(map) {
    var tupleArray = [];
    var result = {};
    for (var key in map) tupleArray.push([key, map[key]]);
    tupleArray.sort(function (a, b) {
        return b[1] - a[1]
    });
    for (var i = 0; i < 10; i++) {
        result[tupleArray[i][0]] = tupleArray[i][1]
    }
    return result;
}
//=============================================================My Research Api===============================================================
//using get route
app.get('/Researchapi/Health/myresearch', function (req, res) {
    FetchFroms3link(res, req.query["userName"]);
});
//============================================================Fetch From Database==========================================================
function FetchFroms3link(res, search) {
    console.log(search)
    var allDataset = [];
    var issearched = false;
    if (search == undefined || search == "") {
        res.status(404).send("No Record Found!!")
    }
    else {
        var params = {
            TableName: "UserResearch",
            FilterExpression: "#K=:createdBy",
            ExpressionAttributeNames: {
                "#K": "createdBy"
            },
            ExpressionAttributeValues: {
                ":createdBy": search
            }
        };
        docClient.scan(params, onScan);
        function onScan(err, data) {
            if (err) {
                console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
            } else {
                data.Items.forEach(function (item) {
                    item.source = "healthdata.gov";
                    allDataset.push(item);
                });
            }
            res.status(200).send(allDataset);
        }
    }
}
//---------------------------------------------------------save Api-----------------------------------------------------------------------------
//using get route
app.get('/Researchapi/Health/save', function (req, res) {
    //  searchindb(res, req.query["Searchkey"],req.query["userName"]);
    try {
        // console.log(req);
        var query = JSON.parse(req.headers["query"]);
        var userName = req.headers["username"];
        console.log(query)
        if (query.ids) {
            for (var k = 0; k < query.ids.length; k++) {
                var search = query.ids[k].value;
                searchindb(res, search, userName);
            }
            res.status(200).send("OK");
        }
    }
    catch (e) { res.status(500).send("Failed"); }

})
//------------------------------------------------Getting Data From Our API------------------------------------------------
function searchindb(res, search, userName) {
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
                awsurlss = [];
                if (item.distribution != undefined) {
                    awsurlss = [];
                    for (var i = 0; i < item.distribution.length; i++) {
                        if (item.distribution[i].title == "csv")
                            getdata(item.distribution[i].accessURL, item.title + "_Distribution_" + i, item.identifier, item.description, userName, item.title, item.distribution[i].title)
                    }

                    // saveDynamodb(awsurlss,"Testing","fert", "sssssss","admin","Admin");
                }

            });
            issearched = false;
        }
    }

}
//------------------------------------------------Extract Data from Clicked Url------------------------------------------------
function getdata(url, title, identifier, description, userName, titlenew, format) {

    console.log(url);
    datta = Request.get({
        "headers": {
            "content-type": "application/json",
            "Origin": '*'
        },
        "url": url,
    }, (error, response, body) => {
        if (error) {
            return console.log(error);
        }
        else {
            saveData(body, title, url, identifier, description, userName, titlenew, format);
        }
    })

}
//------------------------------------------------Store Data behind Url into S3--------------------------------------------
AWS.config.update(awsConfig);
var s3 = new AWS.S3();
function saveData(body, title, url, identifier, description, userName, titlenew, format) {
    console.log(format + "++++++" + url);

    console.log('Data behind Link is successfully dumped into s3');
    var myBucket = 'userresearch.data';
    var myKey = title + "." + format,
        params = { Bucket: myBucket, Key: myKey, Body: body };
    s3.putObject(params, function (err, data) {

        if (err) {
            console.log(err)
        } else {

            getawsurl(myKey, title, url, identifier, description, userName, titlenew, format);
        }
    });

}
//---------------------------------------Getting Url From Aws -------------------------------------------------------------
function getawsurl(myKey, title, url, identifier, description, userName, titlenew, format) {
    var params = {
        Bucket: 'userresearch.data',
        Key: myKey,
    };
    awsurl = s3.getSignedUrl('getObject', params).split("?")[0];

    updatecatalog(identifier);
    awsurlss.push(awsurl);
    saveDynamodb(awsurl, titlenew, url, identifier, description, userName);


}
//------------------------------------------------Passing url to DynamoDb---------------------------------------------------
let saveDynamodb = function (awsurl, title, url, identifier, description, userName) {
    AWS.config.update(awsConfigDynamo);
    let docClient = new AWS.DynamoDB.DocumentClient();

    var input = {
        "ID": title + userName,
        "title": title, "createdBy": userName, "createdOn": new Date().toString(),
        "updatedBy": userName.toLowerCase(),
        "accessUrl": url,
        "awsURL": awsurlss,
        "Description": description,
        "identifier": identifier
    };
    var params = {
        TableName: "UserResearch",
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
        UpdateExpression: "SET S3Link = :label",
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
//--------------------------------------------Collaboration Api-------------------------------------------------

const s3Config = new AWS.S3({
    region: "us-east-2",
    Bucket: "usercollaboration"
});
var uuid4 = uuidv4()
//multer 
const multerS3Config = multerS3({
    s3: s3Config,
    bucket: "usercollaboration",
    metadata: function (req, file, cb) {
        cb(null, {

            fieldName: file.fieldname
        });
    },
    key: function (req, file, cb) {

        // console.log(req)
        cb(null, uuid4 + "_" + file.originalname)
    }
});
//===================================================================
var upload = multer({
    storage: multerS3Config
})

//=======================================================================

app.post('/Researchapi/Health/collaboration', upload.any(), function (req, res) {

    console.log("file has been successfully uploaded");
    save(req)
    res.end("successfully-uploaded")
});

//================================================================= save function-----------------------------------------------------------

function save(req) {
    //console.log(req.body);
    var URLS = [];
    for (var i = 0; i < req.files.length; i++) {
        URLS.push(req.files[i].location)
    }

    var input = {
        "id": uuidv4() + "_" + req.body.username.toString(),
        "@type": "csv",
        "accessLevel": "public",
        "description": req.body.comment.toString(),
        "distribution": [
            {
                "@type": "health",
                "downloadURL": URLS[0].toString(),
                "format": "csv",
                "accessURL": URLS[0].toString(),
                "title": "csv"
            }
        ],
        "keyword":
            []
        ,
        "license": "http://opendefinition.org/licenses/odc-odbl/",
        "modified": new Date().toString(),
        "publisher": {
            "id": req.body.username.toString(),
            "name": req.body.username.toString()
        },

        "S3Link": "Yes",
        "comment": req.body.comment.toString(),
        "Username": req.body.username.toString(),
        "title": req.body.title.toString(),
        "Updated_Date": new Date().toString(),
        "View": req.body.view.toString(),
        "S3_Url": URLS,
        "Metadata_Info_Ids": req.body.metadata_Info_Ids.toString(),
        "usercomments": []
    }
    var params = {
        TableName: "UserCollaboration",
        Item: input
    };
    AWS.config.update(awsConfigDynamo);
    let docClient = new AWS.DynamoDB.DocumentClient();
    docClient.put(params, function (err, data) {

        if (err) {
            console.log("MetaData not inserted Yet " + JSON.stringify(err, null, 2));
        } else {
            console.log("Dataset inserted To Collaboration successfully");
            // console.log(row.distribution[index].accessURL);    
            console.log("=========================")
        }
    });
}


//=========================================================User Collaboration Api==========================================================
////using get route
app.get('/Researchapi/Health/usercollaboration', function (req, res) {
    if (req.query["key"] != undefined) { FetchFromUserCollaboration(res, req.query["key"]); }

});
//---------------------------------------------------------Fetch From Database--------------------------------------------------------------------
function FetchFromUserCollaboration(res, search) {
    console.log(search)
    var allDataset = [];
    var issearched = false;
    var params = {
        TableName: "UserCollaboration",
        FilterExpression: "(contains(#K, :Metadata_Info_Ids))",
        ExpressionAttributeNames: {
            "#K": "Metadata_Info_Ids"
        },
        ExpressionAttributeValues: {
            ":Metadata_Info_Ids": search
        }
    };
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
//=========================================================Comment Api==========================================================================
////using get route
app.post('/Researchapi/Health/Collabration/Comment', function (req, res) {
    if (req.body.key != undefined || req.body.key != "") {
        var key = req.body.key;
        var username = req.body.username;
        var comment = req.body.comment;
        addUserComments(key, username, comment);
    }
    res.status(200).send("comment saved successfully");
});
function addUserComments(ID, username, comment) {

    var usercomments = [{ "username": username, "comment": comment, "commentDate": new Date().toString() }];
    AWS.config.update(awsConfigDynamo);
    let docClient = new AWS.DynamoDB.DocumentClient();
    var params = {
        TableName: "UserCollaboration",
        Key: {
            "id": ID
        },
        UpdateExpression: "SET #usercomments= list_append(#usercomments, :label) ",
        ExpressionAttributeNames: { "#usercomments": "usercomments" },
        ExpressionAttributeValues: {
            ":label": usercomments,
        }, ReturnValues: "UPDATED_NEW"
    };
    docClient.update(params, function (err, data) {
        if (err) {
            console.log("Comments column not updated" + JSON.stringify(err, null, 2));
        } else {
            console.log("Comments column successfully updated");
        }
    });

}
//======================================================================End=========================================================
