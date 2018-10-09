const util = require('util');
const fs = require('fs');
const archiver = require('archiver');
const path = require('path');
const csv = require('fast-csv');
const dateformat = require('dateformat');


class Task {
    get rf() {
        return this._rf;
    }

    set rf(value) {
        this._rf = value;
    }
	
    constructor(request, client, id, type){
		this._id = id;
        this._current_progress = 0;
        this._result = {
            complete: false,
            file_path: '',
            max: 0,
            curr: 0
        };
        this._rf = null;
        executeTask(request, client, this);
    }
	
	executeTask(req, db, context) {
		var start = (req.query.start === undefined) ? 0 : req.query.start;
        var end = (req.query.end === undefined) ? new Date().getTime() : req.query.end;
        var exchange = ((req.query.exchange === undefined) ? [] : req.query.exchange);
        var symbol = ((req.query.symbol === undefined) ? [] : req.query.symbol);
        var format = (req.query.format === undefined) ? 'json' : req.query.format;
        var askbid = '%' + ((req.query.askbid === undefined) ? '' : req.query.askbid) + '%';
        var time = req.query.time;
        var rar_compress = req.query.rar;
        processDataByParams(req, db, context, start, end, exchange, symbol, format, askbid, rar_compress, type, time);
	}

    createSQLQuery(type, exchange, symbol, askbid, start, end) {
        var sql = "";
        switch (type) {
            case 0:
                sql = "SELECT * FROM quatations WHERE (time BETWEEN $1 AND $2)";
                break;
            case 1:
                sql = "SELECT * FROM prices WHERE (time BETWEEN $1 AND $2) AND type LIKE $3";
                break;
            default:
                break;
        }
        if (exchange.length > 0) {
            sql += " AND exchange IN (";
            exchange.forEach(item => {
                sql += "'" + item + "', ";
            });
            sql = sql.substring(0, sql.length - 2);
            sql += ")";
        }
        if (symbol.length > 0) {
            sql += " AND symbol IN (";
            symbol.forEach(item => {
                sql += "'" + item + "', ";
            });
            sql = sql.substring(0, sql.length - 2);
            sql += ")";
        }
        return {
            text: sql,
            values: (type === 0) ? [start, end] : [start, end, askbid]
        }
    }

    processDataByParams(req, db, context, start, end, exchange, symbol, format, askbid, rar_compress, type, time) {
        // **************************** create query with params ****************************
        var sql_query_obj = this.createSQLQuery(type, exchange, symbol, askbid, start, end);
        // ****************************create paths *****************************************
        var file_path = path.join(__dirname, 'file.' + format);
        var zip_path = path.join(__dirname, 'file.zip');
        //*****************************create streams****************************************
        var ws = fs.createWriteStream(file_path);
        var csv_stream = csv.createWriteStream({headers: true});
        var zip_stream = fs.createWriteStream(zip_path);
        //create zip stream and create archiever if needed
        var archive = archiver('zip', {
            zlib: { level: 9 }
        });

        zip_stream.on('close', () => {
            context.result = {
                complete: true,
                file_path: zip_path,
                max: 100,
                curr: 100
            };
        });

        zip_stream.on('end', () => {
            console.log('data has been drained');
        });

        zip_stream.on('warning', (err) => {
            if (err.code === 'ENOENT') {
                // log warning
            } else {
                // throw error
                throw err;
            }
        });

        zip_stream.on('err', (err) => {
            throw err;
        });

        archive.pipe(zip_stream);

        var rows = null;
        csv_stream.pipe(ws);
        var i = 0;
        //****************************** sql query *****************************************
        db.query(sql_query_obj, (err, result_val) => {
            if (err) console.log(err);
            rows = result_val.rows;
            csv_stream.on('data', item => {
                context.current_progress = i;
                var success = false;
                console.log('numbers: ', i, rows.length);
                if (i == rows.length && rar_compress === 'true') {
                    console.log("make file completed");
                    if (rar_compress === 'true'){
                        archive.file(file_path, { name: 'file.csv' });
                        archive.finalize();
                    }
                } else if (i == rows.length && rar_compress !== 'true') {
                    success = true;
                    console.log("make file completed");
                }
                context.result = {
                    complete: success,
                    file_path: file_path,
                    max: rows.length,
                    curr: i
                };
                i++;
            });
            //******************************* write data **************************************
            if (type === 0) {
                csv_stream.write([['Number', 'Date and time', 'Exchange', 'Symbol', 'Volume',
                    'Quote volume', 'price', 'bid', 'ask', 'spread']]);
                rows.forEach((item, index) => {
                    var date_time_event = dateformat(new Date(Number((time === 'internal' ? item.system_time : item.time))),
                        'yyyy-mm-dd HH:MM:ss:l');
                    csv_stream.write([[index + 1, date_time_event,
                        item.exchange, item.symbol, item.volume, item.quote_volume, item.price, item.bid_price,
                        item.ask_price, (item.ask_price - item.bid_price)]]);
                });
            } else if (type === 1) {
                csv_stream.write([['Number', 'Date and time', 'Exchange', 'Symbol', 'Type',
                    'Quantity', 'Price']]);
                rows.forEach((item, index) => {
                    var date_time_event = dateformat(new Date(Number((time === 'internal' ? item.system_time : item.time))),
                        'yyyy-mm-dd HH:MM:ss:l');
                    csv_stream.write([[index + 1, date_time_event,
                        item.exchange, item.symbol, item.type, item.quantity, item.price]]);
                });
            }
        });

    }


    get current_progress() {
        return this._current_progress;
    }

    set current_progress(value) {
        this._current_progress = value;
    }


    get id() {
        return this._id;
    }

    set id(value) {
        this._id = value;
    }


    get result() {
        return this._result;
    }

    set result(value) {
        this._result = value;
    }
}

module.exports = Task;