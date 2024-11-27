import { convertUserTable, convertJobTable, convertCountryTable, convertCategoryTable, convertEducationTable, convertExperienceTable, convertResumeTable, convertCompanyTable, convertAttachmentTable, convertCityTable, convertUserJobsTable, convertJobsTrackTable, convertCertificateTable, calculatePercentageOfUsers, migrateUsersToCompany } from "./convertTable"

export const main = async (mysqlPool: any, mongoClient: any) => {
    try {
        const tasks: any[] = [
            await convertUserTable(mysqlPool, mongoClient, 'punekerkues'),
            await convertUserTable(mysqlPool, mongoClient, 'punedhenes'),
            await convertCompanyTable(mysqlPool, mongoClient),
            await convertAttachmentTable(mysqlPool, mongoClient),
            await convertResumeTable(mysqlPool, mongoClient),
            await convertEducationTable(mysqlPool, mongoClient),
            await convertExperienceTable(mysqlPool, mongoClient),
            await convertCertificateTable(mysqlPool, mongoClient),
            await convertCountryTable(mysqlPool, mongoClient),
            await convertCityTable(mysqlPool, mongoClient),
            await convertCategoryTable(mysqlPool, mongoClient),
            await convertJobTable(mysqlPool, mongoClient),
            await convertUserJobsTable(mysqlPool, mongoClient),
            await convertJobsTrackTable(mysqlPool, mongoClient),
            await calculatePercentageOfUsers(mysqlPool, mongoClient),
            await migrateUsersToCompany(mongoClient),
        ]

        await Promise.all(tasks);
        console.info('All conversions completed successfully. 🚀');
    } catch (error) {
        console.error('Error during table conversion or calculation:', error);
        throw error;
    }
};