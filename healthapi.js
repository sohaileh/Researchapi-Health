const express = require("express");
const bodyParser = require("body-parser");
const Request = require("request");
const fs = require('fs');

//making an express object
const app = express();

//server listen
app.listen(9004, function (req, res) {
});
console.log("server is listening at port 9004");

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

////using get route
app.get('/Researchapi/Health', function (req, res) {
    //console.log(req.query["keyword"]);

    FetchFromFile(res, req.query);
})


//fething function
function FetchFromFile(res, query) {

    var search = query["search"].toLowerCase();
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
                    if ( row.keyword[i].toLowerCase().indexOf(search) !== -1 ) {
                        issearched = true;
                    }
                }
                if ( row.title.toLowerCase().indexOf(search) !== -1  || row.modified == modifieddateT ) {
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
