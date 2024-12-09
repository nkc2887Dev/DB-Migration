import express from "express";
import dotenv from "dotenv";
import path from "path";
dotenv.config()
import { connectionOfMongo } from "./db/connectMongo";
import { connectionOfMySQL } from "./db/connectMySql";
import { main } from "./service/index";
import { renameFilesInFolder } from "./service/script";

const app = express();
(async () => {
    const folderPathResume = path.join(__dirname, "../../../Downloads/cv_jobs/cv")
    renameFilesInFolder(folderPathResume);
})();

(async () => {
    const mongoClient = await connectionOfMongo();
    const mysqlPool = connectionOfMySQL;
    await main(mysqlPool, mongoClient);
})();

app.listen("7855", () => {
    console.info(`server listen on http://localhost:7855/`)
})