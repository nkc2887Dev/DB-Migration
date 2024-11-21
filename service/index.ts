import { convertUserTable, convertJobTable, convertCountryTable, convertCategoryTable, convertEducationTable } from "./convertTable"

export const main = async (mysqlConn: any, mongoClient: any) => {
    // await convertUserTable(mysqlConn, mongoClient);
    // await convertCountryTable(mysqlConn, mongoClient);
    // await convertCategoryTable(mysqlConn, mongoClient);
    await convertEducationTable(mysqlConn, mongoClient);
    await convertJobTable(mysqlConn, mongoClient);
};