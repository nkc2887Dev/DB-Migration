import { convertUserTable, convertJobTable, convertCountryTable, convertCategoryTable, convertEducationTable, convertExperienceTable, convertResumeTable, convertCompanyTable, convertAttachmentTable, convertCityTable, convertUserJobsTable, convertJobsTrackTable, convertCertificateTable, calculatePercentageOfUsers } from "./convertTable"

export const main = async (mysqlConn: any, mongoClient: any) => {
    await convertAttachmentTable(mysqlConn, mongoClient);
    await convertCompanyTable(mysqlConn, mongoClient);
    await convertUserTable(mysqlConn, mongoClient);
    await convertResumeTable(mysqlConn, mongoClient);
    await convertEducationTable(mysqlConn, mongoClient);
    await convertExperienceTable(mysqlConn, mongoClient);
    await convertCertificateTable(mysqlConn, mongoClient);
    await convertCountryTable(mysqlConn, mongoClient);
    await convertCityTable(mysqlConn, mongoClient);
    await convertCategoryTable(mysqlConn, mongoClient);
    await convertJobTable(mysqlConn, mongoClient);
    await convertUserJobsTable(mysqlConn, mongoClient);
    await convertJobsTrackTable(mysqlConn, mongoClient);
    await calculatePercentageOfUsers(mysqlConn, mongoClient);
};