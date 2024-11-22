import { convertUserTable, convertJobTable, convertCountryTable, convertCategoryTable, convertEducationTable, convertExperienceTable, convertResumeTable, convertCompanyTable, convertAttachmentTable, convertCityTable } from "./convertTable"

export const main = async (mysqlConn: any, mongoClient: any) => {
    await convertUserTable(mysqlConn, mongoClient);
    await convertEducationTable(mysqlConn, mongoClient);
    await convertExperienceTable(mysqlConn, mongoClient);
    await convertResumeTable(mysqlConn, mongoClient);
    await convertAttachmentTable(mysqlConn, mongoClient);
    await convertCountryTable(mysqlConn, mongoClient);
    await convertCategoryTable(mysqlConn, mongoClient);
    await convertCompanyTable(mysqlConn, mongoClient);
    await convertCityTable(mysqlConn, mongoClient);
    await convertJobTable(mysqlConn, mongoClient);
};