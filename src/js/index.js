var mysql = require('promise-mysql2');

var connection;
const aws = require('aws-sdk');

exports.handler = async (event) => {
    console.log(event);
    if (typeof connection === 'undefined') {
        try {
            connection = await mysql.createConnection({
                host:  process.env.db_endpoint,
                user: process.env.db_admin_user,
                password: process.env.db_admin_password,
                database: process.env.db_name
                });
        }
        catch(err) {
            console.log(err);
            throw new Error(JSON.stringify({"status": 500, "messages": ['Database connection error']}));
        }
    }

    var rows;
    var fields;
    try {
        [rows, fields] = await connection.query('SELECT * FROM advertiser_campaigns');
    }
    catch(err) {
        console.log(err);
        throw new Error(JSON.stringify({"status": 500, "messages": ['Database query error']}));
    }
    if (rows.length > 0) {
        for(let x = 0; x < rows.length; x++) {
            var docClient = new aws.DynamoDB.DocumentClient()
            var table = "budgets";
            var params = {
                TableName:table,
                Key:{
                    "campaign_id": rows[x].id.toString(),
                },
                UpdateExpression: "set budget = :b, balance = :ba",
                ExpressionAttributeValues:{
                    ":b":rows[x].budget,
                    ":ba": 0,
                },
                ReturnValues:"UPDATED_NEW"
            };
            console.log(rows[x])
            console.log("Updating the item...");
            await docClient.update(params, function(err, data) {
                if (err) {
                    console.error("Unable to update item. Error JSON:", JSON.stringify(err, null, 2));
                }
                else {
                    console.log("UpdateItem succeeded:", JSON.stringify(data, null, 2));
                }
            }).promise();
        }
        return rows;
    }
    else
        throw new Error(JSON.stringify({"status": 404, "messages": ['Publisher does not exist']}));
}