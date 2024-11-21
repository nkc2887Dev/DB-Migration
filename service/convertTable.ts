import bcrypt from "bcrypt";
import { assignDefaultLicence, findOrCreateMaster, migrateProfilePercentage, slugify } from "./helpers";
import countriesJSON from './constant/country.json';
const database = process.env.MONGO_DB;

export const convertUserTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        // Fetch user data from MySQL
        const rolename = '';
        const email = '';
        // const [usersFromMySQL] = await mysqlConn.query(`
        //     SELECT u.id AS user_id, u.*, 
        //            p.id AS people_id, p.*
        //     FROM users u
        //     LEFT JOIN people p ON u.email = p.email WHERE u.email = 'marketing@telegrafi.com'
        // `);
        const [usersFromMySQL] = await mysqlConn.query(`
            SELECT 
                u.id AS user_id, 
                u.*, 
                p.id AS people_id,
                p.*, 
                ru.role_id, 
                r.id AS role_id_in_roles, 
                r.name AS role_name
            FROM role_user ru
            INNER JOIN roles r ON ru.role_id = r.id
            INNER JOIN users u ON ru.user_id = u.id
            LEFT JOIN people p ON u.email = p.email
            WHERE r.name = "${rolename}" AND u.email = "${email}";
        `);

        // Connect to MongoDB
        const collection = db.collection("user");

        // Gender
        const genderData = await db.collection("master").find({ parentCode: "GENDER" }).toArray();
        const genderMapping = genderData.reduce((acc: any, gender: any) => {
            if (gender.name === "Male") {
                acc['M'] = { genderId: gender._id, genderNm: gender.names };
            } else if (gender.name === "Female") {
                acc['F'] = { genderId: gender._id, genderNm: gender.names };
            }

            return acc;
        }, {});

        // Role
        const role = await db.collection("role").findOne({ code: "CANDIDATE" });
        const roleId = role ? role._id : null;
        const miscSetting = await db.collection("settings").findOne({ type: "MISCELLANEOUS" });

        // Prepare data for MongoDB insertion
        const usersToInsert = await Promise.all(usersFromMySQL.map(async (user: any) => {
            const genderInfo = genderMapping[user.gender] || { genderId: null, genderNm: null };
            const [verificationResults] = await mysqlConn.execute(`SELECT user_id, token FROM user_verifications WHERE user_id IN (?)`, [user.people_id]);
            const tokenMapping = verificationResults.reduce((acc: any, verification: any) => {
                acc[verification.user_id] = verification.token;
                return acc;
            }, {});

            const verificationToken = tokenMapping[user.user_id] || null;
            const [companyDetails] = await mysqlConn.query(`SELECT c.* FROM person_companies pc JOIN companies c ON pc.company_id = c.id WHERE pc.person_id = ?; `, [user.people_id]);

            const licenceData = await assignDefaultLicence(user.user_id, db);
            const countProfilePercentage = await migrateProfilePercentage(user, db);

            return {
                importId: "DEV-45912",
                _id: user.user_id,
                email: user.email,

                isActive: ["IMPORTED", "ACTIVE"].includes(user.status),
                emailVerify: {
                    code: Math.floor(100000 + Math.random() * 900000),
                    expireTime: new Date(new Date(user.created_at).getTime() + 10 * 60 * 1000),
                },
                services: {},
                passwords: [
                    {
                        pass: user.password,
                        salt: await bcrypt.genSalt(10),
                        createdAt: new Date(user.created_at),
                        isActive: user.is_verified === 1,
                    },
                ],
                roles: [
                    {
                        roleId: roleId,
                    },
                ],
                createdAt: new Date(user.created_at),
                updatedAt: new Date(user.updated_at),
                emailVerifiedAt: new Date(user.updated_at),
                firstName: user.name,
                lastName: user.last_name,
                name: `${user.name} ${user.last_name}`,
                mobNo: user.phone,
                // countryCode: user.phone,
                dob: user.birthday ? new Date(user.birthday) : null,
                genderNm: genderInfo.genderNm,
                genderId: genderInfo.genderId,
                imageId: user.image_id,
                description: user.description,
                jobTitle: user.job_title,
                tz: "Asia/Kolkata",
                lastLogin: user.last_activity ? new Date(user.last_activity) : null,
                canDel: true,
                offNotification: false,
                canChangePass: true,
                expSnapshot: [],
                licenceId: licenceData?.licenceId || null,
                purchasedPlanId: null,
                unsubscribedJobId: [],
                approvalStatus: miscSetting && !!(miscSetting.candidateAutoApprove) && !miscSetting.candidateAutoApprove ? 1 : 2,
                consentAgree: false,
                isPrimaryAdmin: false,
                isProfileSetupCompleted: false,
                jobs: {
                    appliedJobCount: 0,
                    totalAppliedJobs: 0,
                    jobApplicationLimit: licenceData?.jobApplicationLimit || 0
                },
                qualifications: [{
                    _id: null,
                    qualificationId: null,
                    qualificationNm: {},
                    fieldOfStuNm: {},
                },],
                resumeFileId: "",
                resumes: [{
                    tempId: null,
                    uri: null,
                    oriNm: null,
                    prevUsed: null,
                    createdAt: null,
                },],
                aiSkills: [],
                aiDomain: [],
                countryId: null,
                countryNm: null,
                stateId: null,
                stateNm: null,
                cityId: null,
                cityNm: null,
                customFields: {},
                profileId: null,
                profile: {
                    nm: null,
                    oriNm: null,
                    type: null,
                    exten: null,
                    uri: null,
                    mimeType: null,
                    size: null,
                    sts: null,
                    dimensions: { height: null, width: null },
                    preview: null,
                    createdBy: null,
                },
                experienceIds: {
                    id: "",
                    expNm: {},
                },
                jobLevelId: {
                    id: "",
                    jobLevelNm: {},
                },
                domain: [
                    {
                        id: "",
                        domainNm: {},
                    }
                ],
                skillIds: [
                    {
                        id: "",
                        skillNm: {},
                    }
                ],
                ind: [
                    {
                        id: "",
                        nm: {},
                    }
                ],
                profileCompleted: countProfilePercentage.percentage || 30,
                percentObj: countProfilePercentage.percentObj || {
                    basicDetails: 1,
                    personalDetails: 1,
                    industryDetails: 0,
                    educations: 0,
                    projects: 0,
                    experiences: 0,
                    certificates: 0,
                    accomplishments: 0,
                },
                ignoreCompanies: [],
                tokens: [
                    {
                        token: verificationToken ? verificationToken : null,
                        validateTill: verificationToken ? new Date(new Date(user.created_at).getTime() + 24 * 60 * 60 * 1000) : null,
                        refreshToken: null,
                        deviceDetail: null,
                    }
                ],
                compId: companyDetails?.[0]?.id || undefined,
                compNm: companyDetails?.[0]?.name || undefined,
            }
        }));

        // Insert data into MongoDB
        const bulkOps = usersToInsert.map(user => ({
            updateOne: {
                filter: { _id: user._id },
                update: { $set: user },
                upsert: true,
            }
        }));

        if (bulkOps.length > 0) {
            await collection.bulkWrite(bulkOps);
        }
        console.info(`Successfully migrated ${usersToInsert.length} users to MongoDB`);
    } catch (error) {
        console.error("Error - convertUserTable", error);
        throw error;
    }
}

export const convertCountryTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [countries] = await mysqlConn.query(`
            SELECT id, name, status, created_at, updated_at
            FROM countries
          `);

        const countriesToAdd: any = [];

        countries.forEach((country: any) => {
            const matchingCountry = countriesJSON.find(
                (jsonCountry: any) => jsonCountry.name === country.name
            );

            if (matchingCountry) {
                countriesToAdd.push({
                    ...matchingCountry,
                    importId: "DEV-45912",
                    _id: country.id,
                    status: country.status,
                    created_at: country.created_at,
                    updated_at: country.updated_at,
                });
            }
        });
        const bulkOps = countriesToAdd.map((country: any) => {
            return {
                updateOne: {
                    filter: { name: country.name },
                    update: { $set: country },
                    upsert: true,
                },
            };
        });

        if (bulkOps.length > 0) {
            await db.collection("country").bulkWrite(bulkOps);
        }
        console.info(`Successfully migrated ${countriesToAdd.length} country to MongoDB`);
    } catch (error) {
        console.error("Error - convertCountryTable", error);
        throw error;
    }
}

export const convertCategoryTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [categoriesFromMySQL] = await mysqlConn.query('SELECT * FROM categories');

        const bulkOperations = categoriesFromMySQL.map((category: any) => {
            const categoryData = {
                importId: "DEV-45912",
                _id: category.id,
                nm: category.name,
                slug: slugify(category.name),
                fileId: null,
                skillIds: [],
                ind: [],
                domain: [],
                isActive: true,
                createdBy: null,
                updatedBy: [],
                aiSkills: [],
                aiDomain: [],
                createdAt: new Date(category.created_at),
                updatedAt: new Date(category.updated_at),
            };

            return {
                updateOne: {
                    filter: { slug: categoryData.slug },
                    update: { $set: categoryData },
                    upsert: true,
                },
            };
        });

        if (bulkOperations.length > 0) {
            await db.collection("category").bulkWrite(bulkOperations);
        }
        console.info(`Successfully migrated ${categoriesFromMySQL.length} Category to MongoDB`);
    } catch (error) {
        console.error("Error - convertCategoryTable", error);
        throw error;
    }
};

export const convertEducationTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [educationRecords] = await mysqlConn.query(`
            SELECT 
            e.id AS education_id, 
            e.school AS institution_name, 
            e.degree AS qualification, 
            e.field_of_study, 
            e.location, 
            e.period_start, 
            e.period_end, 
            e.is_present AS still_active, 
            e.person_id AS user_id,
            e.created_at, 
            e.updated_at, 
            e.degree
            FROM 
            education e
            JOIN 
            people p ON e.person_id = p.id
            LIMIT 1;
          `);

        const bulkOperations = await Promise.all(educationRecords.map(async (education: any) => {
            const instiNmCache: any = await findOrCreateMaster(education.institution_name, "INSTITUTE_UNIVERSITY", db);
            const qualificationNmCache: any = await findOrCreateMaster(education.qualification, "EMPLOYEE_QUALIFICATION", db);
            const fieldOfStuNmCache: any = await findOrCreateMaster(education.field_of_study, "FIELD_OF_STUDY", db);
            return {
                updateOne: {
                    filter: { _id: education.education_id },
                    update: {
                        $set: {
                            importId: "DEV-45912",
                            _id: education.education_id,
                            userId: education.user_id,
                            institutionId: instiNmCache?._id,
                            instiNm: {
                                "en": education.institution_name,
                                "id": education.institution_name,
                            },
                            qualificationId: qualificationNmCache?._id,
                            qualificationNm: {
                                "en": education.qualification,
                                "id": education.qualification,
                            },
                            fieldOfStuId: fieldOfStuNmCache?._id,
                            fieldOfStuNm: {
                                "en": education.field_of_study,
                                "id": education.field_of_study,
                            },
                            date: education.created_at,
                            from: education.period_start,
                            to: education.period_end,
                            stillActive: education.still_active,
                            createdAt: new Date(education.created_at),
                            updatedAt: new Date(education.updated_at),
                            customFields: {}
                        }
                    },
                    upsert: true
                }
            };
        }));
        if (bulkOperations.length > 0) {
            await db.collection("educations").bulkWrite(bulkOperations);
        }
        console.info(`Successfully migrated ${educationRecords.length} Education to MongoDB`);

    } catch (error) {
        console.error("Error - convertEducationTable", error);
        throw error;
    }
};

export const convertJobTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        console.log("convertJobTable");
    } catch (error) {
        console.error("Error - convertJobTable", error);
        throw error;
    }
};