require('dotenv').config();
const express = require('express');
const mysql = require('mysql')

// we need connections pool to make sure each request work with different connection
const pool = mysql.createPool({
    connectionLimit: 10,
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
});

// create table and record if they don't exist
pool.query('CREATE TABLE IF NOT EXISTS WALLET (ID INT NOT NULL PRIMARY KEY, BALANCE INT DEFAULT 0);', (err) => {
    pool.query('REPLACE INTO WALLET SET ?', { id: 1, balance: 10 });
});

const app = express()

// end point that have concurrency problem
app.put('/problem/:id/:balance', function (req, res) {
    const id = Number(req.params.id);
    const balance = Number(req.params.balance);
    pool.getConnection(function (err, connection) {
        connection.beginTransaction(function (err) {
            connection.query(`SELECT * FROM WALLET WHERE ID=${id};`, (error1, rows1) => {
                console.log('[PROBLEM] query:', rows1);
                connection.query(`UPDATE WALLET SET BALANCE = ${rows1[0].BALANCE + balance} WHERE ID = ${id};`, (error2, rows2) => {
                    console.log('[PROBLEM]update :', rows2);
                    connection.commit(function (err) {
                        if (err) {
                            return connection.rollback(function () {
                                throw err;
                            });
                        }
                        connection.query(`SELECT * FROM WALLET WHERE ID=${id};`, function (error3, rows3) {
                            console.log('[PROBLEM]confirmation query:', rows3);
                            res.json({ row: rows3[0] })
                        })
                    });

                })
            })
        })
    })
})

// endpoint has the concurrency problem solution
app.put('/solution/:id/:balance', function (req, res) {
    const id = Number(req.params.id);
    const balance = Number(req.params.balance);
    pool.getConnection(function (err, connection) {
        connection.beginTransaction(function (err) {
            // check the  `FOR UPDATE` keywords
            connection.query(`SELECT * FROM WALLET WHERE ID=${id} FOR UPDATE;`, function (error1, rows1) {
                console.log('[SOLUTION] query:', rows1);
                connection.query(`UPDATE WALLET SET BALANCE = ${rows1[0].BALANCE + balance} WHERE ID = ${id};`, (error2, rows2) => {
                    console.log('[SOLUTION] update:', rows2);
                    connection.commit(function (err) {
                        if (err) {
                            return connection.rollback(function () {
                                throw err;
                            });
                        }
                        connection.query(`SELECT * FROM WALLET WHERE ID=${id};`, function (error3, rows3) {
                            console.log('[SOLUTION] confirmation query:', rows3);
                            res.json({ row: rows3[0] })
                        })
                    });
                })
            })
        })
    })
})

app.listen(3030)