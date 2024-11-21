import express from "express";
import dotenv from "dotenv";
dotenv.config()
import { connectionOfMongo } from "./db/connectMongo";
import { connectionOfMySQL } from "./db/connectMySql";

const app = express();

(async () => {
    await connectionOfMongo();
    await connectionOfMySQL();
})();

app.listen("7855", () => {
    console.info(`server listen on http://localhost:7855/`)
})