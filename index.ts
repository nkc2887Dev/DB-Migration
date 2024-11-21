import express from "express";
import dotenv from "dotenv";
dotenv.config()
import { connectionOfMongo } from "./db/connectMongo";
import { connectionOfMySQL } from "./db/connectMySql";
import { main } from "./service/index";

const app = express();

(async () => {
    const mongoClient = await connectionOfMongo();
    const mysqlConn = await connectionOfMySQL();
    await main(mysqlConn, mongoClient);
})();

app.listen("7855", () => {
    console.info(`server listen on http://localhost:7855/`)
})