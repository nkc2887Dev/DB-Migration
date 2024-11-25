import bcrypt from "bcrypt";
import { addCompaniesToEmployer, assignDefaultLicence, createCode, findOrCreateMaster, makeItYopmail, migrateProfilePercentage, parseAddress, slugify, updateCompanyWithUserDetails, updateUserExperienceWithDetails, updateUserQualificationsWithDetails, updateUserResumeWithDetails, updateUserWithCompanyDetails } from "./helpers";
import countriesJSON from './constant/country.json';
const database = process.env.MONGO_DB;
import _ from "lodash";

type MimeTypes = {
    pdf: string;
    csv: string;
    doc: string;
    docx: string;
};

export const convertUserTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        // const rolename = 'punekerkues';
        const rolename = 'punedhenes';
        // const rolename = 'admin';

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
            WHERE r.name = ?;
        `, [rolename]);

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
        const roleCode = {
            "punekerkues": "CANDIDATE",
            "punedhenes": "EMPLOYER",
            "admin": "ADMIN",
        }
        const role = await db.collection("role").findOne({ code: roleCode[rolename] });
        const roleId = role ? role._id : null;
        const miscSetting = await db.collection("settings").findOne({ type: "MISCELLANEOUS" });

        const chunkSize = Number(process.env.CHUNK);
        const userChunks = _.chunk(usersFromMySQL, chunkSize);

        for (const chunk of userChunks) {
            const usersToInsert = await Promise.all(chunk.map(async (user: any) => {
                if (!user?.email) return;
                if (process.env.MAKE_IT_YOPMAIL === 'true') {
                    user.email = makeItYopmail(user.email)
                }

                const genderInfo = genderMapping[user.gender] || { genderId: null, genderNm: null };
                const [verificationResults] = await mysqlConn.execute(`SELECT user_id, token FROM user_verifications WHERE user_id IN (?)`, [user.people_id]);
                const tokenMapping = verificationResults.reduce((acc: any, verification: any) => {
                    acc[verification.user_id] = verification.token;
                    return acc;
                }, {});

                const verificationToken = tokenMapping[user.user_id] || null;

                let companyDetails
                if (roleCode[rolename] === 'EMPLOYER') {
                    companyDetails = await addCompaniesToEmployer(user, mysqlConn, db);
                }

                const licenceData = await assignDefaultLicence(user.user_id, db);
                const countProfilePercentage = await migrateProfilePercentage(user, db);

                console.info(`Processing user: ${user.name} (Email: ${user.email})`);

                return {
                    importId: "DEV-45912",
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
                    roles: [{ roleId: roleId }],
                    createdAt: new Date(user.created_at),
                    updatedAt: new Date(user.updated_at),
                    emailVerifiedAt: new Date(user.updated_at),
                    firstName: user.name,
                    lastName: user.last_name,
                    name: `${user.name} ${user.last_name}`,
                    mobNo: user.phone,
                    dob: user.birthday ? new Date(user.birthday) : null,
                    genderNm: genderInfo.genderNm,
                    genderId: genderInfo.genderId,
                    imageId: user.image_id,
                    description: user.description,
                    jobTitle: user.job_title,
                    tz: "Asia/Kolkata",
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
                    qualifications: [],
                    resumeFileId: "",
                    resumes: [],
                    aiSkills: [],
                    aiDomain: [],
                    experienceIds: {},
                    jobLevelId: {},
                    domain: [],
                    skillIds: [],
                    ind: [],
                    profileCompleted: countProfilePercentage.percentage || 10,
                    percentObj: countProfilePercentage.percentObj || {
                        basicDetails: 1,
                        personalDetails: 0,
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

                    ...(roleCode[rolename] === 'EMPLOYER' && {
                        status: 2,
                        type: 1,
                        ...(companyDetails?._id && { compId: companyDetails._id }),
                        ...(companyDetails?.compNm && { compNm: companyDetails.compNm }),
                    }),
                }
            }));

            const bulkOperations = usersToInsert.filter((user): user is NonNullable<typeof user> => user !== null && user !== undefined && user.email);
            const bulkOps = bulkOperations.map(user => {
                return {
                    updateOne: {
                        filter: { importId: "DEV-45912", email: user.email },
                        update: { $set: user },
                        upsert: true,
                    }
                }
            });

            if (bulkOps.length > 0) {
                await db.collection("user").bulkWrite(bulkOps);
            }
            await updateCompanyWithUserDetails(bulkOperations, db);
        }
        console.info(`Successfully migrated ${usersFromMySQL.length} users to MongoDB`);
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
                    status: country.status,
                    created_at: country.created_at,
                    updated_at: country.updated_at,
                });
            }
        });
        const bulkOps = countriesToAdd.map((country: any) => {
            console.info(`Processing country: ${country.name}`);
            return {
                updateOne: {
                    filter: { importId: "DEV-45912", name: country.name },
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
            console.info(`Processing category: ${category.name} (Slug: ${categoryData.slug})`);

            return {
                updateOne: {
                    filter: { importId: "DEV-45912", slug: categoryData.slug },
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
            e.degree,
            p.email
            FROM 
            education e
            JOIN 
            people p ON e.person_id = p.id
            WHERE e.id = "a6200fa2-cc52-430f-b141-c8b65649ace7";
          `);

        const bulkOperations = await Promise.all(educationRecords.map(async (education: any) => {
            const instiNmCache: any = await findOrCreateMaster(education.institution_name, "INSTITUTE_UNIVERSITY", db);
            const qualificationNmCache: any = await findOrCreateMaster(education.qualification, "EMPLOYEE_QUALIFICATION", db);
            const fieldOfStuNmCache: any = await findOrCreateMaster(education.field_of_study, "FIELD_OF_STUDY", db);

            const user = await db.collection("user").findOne({ email: education.email });

            console.info(`Processing education: ${education.qualification}`);

            return {
                importId: "DEV-45912",
                userId: user?._id || null,
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
            };
        }));

        const bulkOps = bulkOperations.map(bulkOp => {
            return {
                updateOne: {
                    filter: {
                        userId: bulkOp.userId,
                        institutionId: bulkOp.institutionId,
                        qualificationId: bulkOp.qualificationId,
                        fieldOfStuId: bulkOp.fieldOfStuId,
                        from: bulkOp.from,
                        to: bulkOp.to
                    },
                    update: { $set: bulkOp },
                    upsert: true,
                }
            }
        });

        if (bulkOps.length > 0) {
            await db.collection("educations").bulkWrite(bulkOps);
        }
        await updateUserQualificationsWithDetails(bulkOperations, db)
        console.info(`Successfully migrated ${bulkOps.length} Education to MongoDB`);

    } catch (error) {
        console.error("Error - convertEducationTable", error);
        throw error;
    }
};

export const convertExperienceTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [experiencesRecords] = await mysqlConn.query(`
            SELECT we.*, p.* 
            FROM work_experiences we
            JOIN people p ON we.person_id = p.id
            WHERE we.person_id = '58f9e998-e8c0-4200-8318-5299872ccdbc'
            `);

        const bulkOperations = await Promise.all(experiencesRecords.map(async (experience: any) => {
            const user = await db.collection("user").findOne({ email: experience.email })
            console.info(`Processing experience: ${experience.title}`);

            return {
                importId: "DEV-45912",
                userId: user?._id || null,
                title: experience.title,
                comNm: experience.company_name,
                countryNm: experience.location,
                from: experience.period_start,
                to: experience.period_end,
                stillActive: !!experience.is_present,
                desc: experience.responsibilities,
                createdAt: experience.created_at,
                updatedAt: experience.updated_at,
            };
        }));
        const bulkOps = bulkOperations.map(bulkOp => {
            return {
                updateOne: {
                    filter: {
                        userId: bulkOp.userId,
                        title: bulkOp.title,
                        comNm: bulkOp.comNm,
                    },
                    update: { $set: bulkOp },
                    upsert: true,
                }
            }
        });

        if (bulkOps.length > 0) {
            await db.collection("experiences").bulkWrite(bulkOps);
        }
        await updateUserExperienceWithDetails(bulkOperations, db)
        console.info(`Successfully migrated ${bulkOps.length} Experiences to MongoDB`);

    } catch (error) {
        console.error("Error - convertExperienceTable", error);
        throw error;
    }
};

export const convertResumeTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [resumesRecords] = await mysqlConn.query(`
            SELECT r.*, p.email 
            FROM resumes r
            JOIN people p ON r.person_id = p.id
        `);
        const mimeType: MimeTypes = {
            'pdf': 'application/pdf',
            'csv': 'text/csv',
            'doc': 'application/msword',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        };

        const getMimeTypeFromFileName = (fileName: any) => {
            const ext = fileName.split('.').pop().toLowerCase();
            if (ext && ext in mimeType) {
                return mimeType[ext as keyof MimeTypes];
            }
            return 'application/octet-stream';
        };
        const bulkOperations = await Promise.all(resumesRecords.map(async (resume: any) => {
            const user = await db.collection("user").findOne({ email: resume.email });
            resume.person_attributes = resume.person_attributes ? JSON.parse(resume.person_attributes) : null;
            const type = resume?.cv_path ? getMimeTypeFromFileName(resume.cv_path) : null;

            console.info(`Processing resume: ${resume.title}`);

            return {
                importId: "DEV-45912",
                userId: user?._id || null,
                nm: resume.title,
                oriNm: resume.name || resume.title,
                type: type,
                exten: resume?.cv_path ? resume.cv_path.split('.').pop() : null,
                uri: resume.cv_path,
                mimeType: type,
                size: resume.person_attributes ? resume.person_attributes?.size : 0,
                sts: resume.person_attributes ? resume.person_attributes?.sts : 2,
                dimensions: resume.person_attributes ? resume.person_attributes?.dimensions : {},
                createdAt: resume.created_at,
                updatedAt: resume.updated_at,
            };
        }));
        const bulkOps = bulkOperations.map(bulkOp => {
            return {
                updateOne: {
                    filter: {
                        userId: bulkOp.userId,
                        nm: bulkOp.nm,
                    },
                    update: { $set: bulkOp },
                    upsert: true,
                }
            }
        });

        if (bulkOps.length > 0) {
            await db.collection("file").bulkWrite(bulkOps);
        }
        await updateUserResumeWithDetails(bulkOperations, db);
        console.info(`Successfully migrated ${bulkOps.length} Resumes to MongoDB`);
    } catch (error) {
        console.error("Error - convertResumeTable", error);
        throw error;
    }
};

export const convertAttachmentTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [attachmentsRecords] = await mysqlConn.query(`
                SELECT atc.*, p.email AS userEmail  
                FROM attachments atc
                JOIN users p ON atc.created_by = p.id
            `);

        const bulkOperations = await Promise.all(attachmentsRecords.map(async (attach: any) => {
            const user = await db.collection("user").findOne({ email: attach.userEmail });

            console.info(`Processing resume: ${attach.name}`);

            return {
                importId: "DEV-45912",
                userId: user?._id || null,
                nm: attach.name,
                oriNm: attach.name,
                type: attach.mime_type,
                exten: attach.path.split('.').pop(),
                uri: attach.path,
                sts: 2,
                mimeType: attach.mime_type,
                createdAt: attach.created_at,
                updatedAt: attach.updated_at,
            };
        }));
        const bulkOps = bulkOperations.map(bulkOp => {
            return {
                updateOne: {
                    filter: {
                        userId: bulkOp.userId,
                        nm: bulkOp.nm,
                    },
                    update: { $set: bulkOp },
                    upsert: true,
                }
            }
        });

        if (bulkOps.length > 0) {
            await db.collection("file").bulkWrite(bulkOps);
        }
        console.info(`Successfully migrated ${bulkOps.length} Attachments to MongoDB`);
    } catch (error) {
        console.error("Error - convertAttachmentTable", error);
        throw error;
    }
};

export const convertCompanyTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [companiesRecords] = await mysqlConn.query(`SELECT
                cm.id AS companyId,
                cm.name,
                cm.slug,
                cm.nui AS licenceNo,
                cm.description AS aboutUs,
                cm.phone AS phone,
                cm.email,
                cm.address,
                cm.website AS compWebURL,
                cm.city AS cityNm,
                cm.country AS countryNm,
                cm.logo_id AS logoId,
                cm.created_by AS updatedBy,
                cm.extra_attributes AS customFields,
                cm.deleted_at AS deletedAt,
                cm.created_at AS createdAt,
                cm.updated_at AS updatedAt,
                cm.og_image_id AS bannerId,
                cm.is_visible AS isActive,
                cm.video_url,
                cm.vr_url,
                p.email AS employerEmail,
                u.email AS userEmail,
                att.name AS logoURL,
                att.created_by AS attachCreateBy,
                    JSON_OBJECT(
                    'personId', pc.person_id,
                    'companyId', pc.company_id,
                    'role', pc.role
                ) AS personCompanies
            FROM companies cm
            LEFT JOIN people p ON cm.created_by = p.id
            LEFT JOIN users u ON cm.created_by = u.id
            LEFT JOIN person_companies pc ON cm.created_by = pc.person_id
            LEFT JOIN attachments att ON cm.logo_id = att.id;
        `);

        const bulkOperations = await Promise.all(companiesRecords.map(async (company: any) => {
            const user = await db.collection("user").findOne({ email: { $in: [company.employerEmail, company.userEmail] } });
            const logoFile = user && await db.collection("file").findOne({ userId: user._id, nm: company.logoURL, importId: "DEV-45912" });
            const parsedAddress = parseAddress(company.address);

            console.info(`Processing company: ${company.name}`);

            return {
                importId: "DEV-45912",
                compNm: company.name,
                slug: company.slug,
                licenceNo: company.licenceNo || undefined,
                userIds: user?._id ? [user?._id] : [],
                conPer: {
                    nm: company.nm,
                    mobileNo: company.phone,
                    email: company.email,
                    countryCode: company.countryCode
                },
                logoId: logoFile?._id || undefined,
                compWebURL: company.compWebURL,
                compLinkedInURL: company.compLinkedInURL,
                profilePercent: company.profilePercent || 0,
                address: {
                    street: parsedAddress.street,
                    address1: parsedAddress.address1,
                    countryNm: parsedAddress.countryNm,
                    cityNm: parsedAddress.cityNm,
                    zipCode: parsedAddress.zipCode,
                },
                isActive: !!company.isActive,
                benefits: company.benefits || undefined,
                compDomains: company.compDomains || undefined,
                createdBy: user?._id || null,
                isDefault: false,
                createdAt: company.createdAt,
                updatedAt: company.updatedAt,
            };
        }));
        const bulkOps = bulkOperations.map(bulkOp => {
            return {
                updateOne: {
                    filter: {
                        importId: "DEV-45912",
                        compNm: bulkOp.compNm,
                        slug: bulkOp.slug,
                    },
                    update: { $set: bulkOp },
                    upsert: true,
                }
            }
        });

        if (bulkOps.length > 0) {
            await db.collection("company").bulkWrite(bulkOps);
        }
        // await updateUserWithCompanyDetails(bulkOperations, db);
        console.info(`Successfully migrated ${bulkOps.length} Companies to MongoDB`);
    } catch (error) {
        console.error("Error - convertCompanyTable", error);
        throw error;
    }
};

export const convertCityTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [cities] = await mysqlConn.query(`
            SELECT 
                c.*, 
                ctr.name AS countryName 
            FROM cities c
            LEFT JOIN countries ctr ON c.country_id = ctr.id
        `);

        const citiesToAdd = await Promise.all(cities.map(async (city: any) => {
            const country = await db.collection("country").findOne({ name: city.countryName });

            console.info(`Processing city: ${city.name}`);

            return {
                importId: "DEV-45912",
                name: city.name,
                code: await createCode(city.name),
                countryId: country?._id || null,
                countryNm: country?.name || null,
                stateNm: city.name,
                canDel: true,
                isDefault: false,
                isActive: !!(city.status === "ACTIVE"),
                created_at: city.created_at,
                updated_at: city.updated_at,
            };
        }));
        const bulkOps = citiesToAdd.map((city: any) => {
            return {
                updateOne: {
                    filter: { importId: "DEV-45912", name: city.name, countryNm: city.countryNm },
                    update: { $set: city },
                    upsert: true,
                },
            };
        });

        if (bulkOps.length > 0) {
            await db.collection("city").bulkWrite(bulkOps);
        }
        console.info(`Successfully migrated ${bulkOps.length} Cities to MongoDB`);
    } catch (error) {
        console.error("Error - convertCityTable", error);
        throw error;
    }
};

export const convertJobTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [jobsRecords] = await mysqlConn.query(`
            SELECT 
              j.id, 
              j.title, 
              j.slug, 
              j.open_positions AS totalVacancy, 
              j.status AS isActive, 
              j.message AS additionalBenefits, 
              j.responsibilities, 
              j.qualifications, 
              j.contract_type, 
              j.apply_url AS redirectUrl, 
              j.city_id AS cityId, 
              j.category_id, 
              j.company_id AS compId, 
              j.created_by AS createdBy, 
              j.approved, 
              j.start_at AS activatedAt, 
              j.expires_at AS expiredAt, 
              j.reorder_at, 
              j.deleted_at AS deletedAt, 
              j.created_at AS createdAt, 
              j.updated_at AS updatedAt, 
              j.og_image_id, 
              j.da_image_id, 
              j.plan_id, 
              j.cta_text, 
              j.cta_link, 
              j.cta_color,
              -- Joining category, city, company, user, and plan
              c.name AS categoryName,
              ct.name AS cityName,
              co.name AS companyName,
              co.slug AS compSlug,
              u.email AS createdByEmail,
              p.title AS planName
            FROM jobs j
            LEFT JOIN categories c ON j.category_id = c.id
            LEFT JOIN cities ct ON j.city_id = ct.id
            LEFT JOIN companies co ON j.company_id = co.id
            LEFT JOIN users u ON j.created_by = u.id
            LEFT JOIN plans p ON j.plan_id = p.id
            WHERE j.id = '24d6b234-da50-4a73-8e34-a277deb75e14'
          `);

        const bulkOperations = await Promise.all(jobsRecords.map(async (job: any) => {
            const contractCache: any = await findOrCreateMaster(job.contract_type, "TYPE_OF_EMPLOYMENT", db);
            const company = await db.collection("company").findOne({ slug: job.compSlug, name: job.companyName, importId: "DEV-45912" });
            const category = await db.collection("category").findOne({ nm: job.categoryName, importId: "DEV-45912" });
            const city = await db.collection("city").findOne({ name: job.cityName, importId: "DEV-45912" });
            const user = await db.collection("user").findOne({ email: job.createdByEmail, importId: "DEV-45912" });

            console.info(`Processing job: ${job.title}`);

            return {
                importId: "DEV-45912",
                title: job.title,
                slug: job.slug,
                totalVacancy: job.totalVacancy,
                desc: job.desc,
                additionalBenefits: job.additionalBenefits,
                isActive: !!(job.isActive === "ACTIVE"),
                isDraft: !job.approved,
                activatedAt: job.activatedAt,
                expiredAt: job.expiredAt,
                deletedAt: job.deletedAt,
                createdBy: user?._id,
                typeOfEmpId: contractCache._id,
                typeOfEmpNm: contractCache.names,
                compId: company?._id,
                compNm: company?.name,
                categoryIds: [
                    {
                        id: category._id,
                        Nm: {
                            "en": category.nm,
                            "id": category.nm
                        },
                    },
                ],
                loc: {
                    cityId: city._id,
                    cityNm: city.name,
                    countryNm: city.countryNm,
                    countryId: city.countryId
                },
                redirectUrl: job.redirectUrl,
                useRedirectUrl: false,
                createdAt: job.createdAt,
                updatedAt: job.updatedAt,
                isShowEmpNm: true,
                matchedCount: 0,
                appliedCount: 0,
                hiredCount: 0,
                unSubscribedCount: 0,
            };
        }));
        const bulkOps = bulkOperations.map(bulkOp => {
            return {
                updateOne: {
                    filter: {
                        importId: "DEV-45912",
                        title: bulkOp.title,
                        slug: bulkOp.slug,
                        createdBy: bulkOp.createdBy,
                    },
                    update: { $set: bulkOp },
                    upsert: true,
                }
            }
        });

        if (bulkOps.length > 0) {
            await db.collection("jobs").bulkWrite(bulkOps);
        }
        console.info(`Successfully migrated ${bulkOps.length} Jobs to MongoDB`);
    } catch (error) {
        console.error("Error - convertJobTable", error);
        throw error;
    }
};

export const convertUserJobsTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [appliesRecords] = await mysqlConn.query(`
            SELECT 
                a.*, 
                r.title AS resumeTitle,
                j.title AS jobTitle,
                j.slug AS jobSlug,
                p.email  AS userEmail
            FROM 
                applies a
            LEFT JOIN 
                resumes r ON a.resume_id = r.id
            LEFT JOIN 
                jobs j ON a.job_id = j.id
            LEFT JOIN 
                people p ON a.person_id = p.id
            WHERE a.id="49865d6a-46f4-4479-801b-52317de99cf3";
            `,)

        const statusMapping: any = {
            PENDING: 'APPLIED',
            SELECTED: 'HIRED',
            REFUSED: 'REJECTED'
        };
        const statusMaster = await db.collection("master").find({ parentCode: "APPLICATION_STATUS" }).toArray();
        const statusDict = statusMaster.reduce((acc: any, status: any) => {
            acc[status.code] = status;
            return acc;
        }, {});

        const bulkOperations = await Promise.all(appliesRecords.map(async (apply: any) => {
            const user = await db.collection("user").findOne({ email: apply.userEmail, importId: "DEV-45912" });
            const job = await db.collection("jobs").findOne({ title: apply.jobTitle, slug: apply.jobSlug, importId: "DEV-45912" });

            const status: any = statusDict[statusMapping[apply.status]] || null;

            console.info(`Processing userJobs: ${apply.userEmail}`);

            return {
                importId: "DEV-45912",
                jobId: job?._id || null,
                jobTitle: job?.title || null,
                userId: user?._id || null,
                resumeObj: user?.resumes ? user.resumes.filter((resume: any) => resume.oriNm === apply.resumeTitle)[0] : {},
                questions: [],
                statusId: status?._id,
                statusNm: status?.names,
                createdBy: user?._id || null,
                updatedBy: user?._id ? [user?._id] : [],
                createdAt: apply.created_at,
                updatedAt: apply.updated_at,
            };
        }));
        const bulkOps = bulkOperations.map(bulkOp => {
            return {
                updateOne: {
                    filter: {
                        importId: "DEV-45912",
                        jobId: bulkOp.jobId,
                        userId: bulkOp.userId
                    },
                    update: { $set: bulkOp },
                    upsert: true,
                }
            }
        });

        if (bulkOps.length > 0) {
            await db.collection("userjobs").bulkWrite(bulkOps);
        }
        console.info(`Successfully migrated ${bulkOps.length} UserJobs to MongoDB`);
    } catch (error) {
        console.error("Error - convertUserJobsTable", error);
        throw error;
    }
};

export const convertJobsTrackTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [bookmarksRecords] = await mysqlConn.query(`
            SELECT
                pb.*,
                j.title AS jobTitle,
                j.slug AS jobSlug,
                p.email  AS userEmail
            FROM 
                person_bookmarks pb
            LEFT JOIN 
                people p ON pb.person_id = p.id
            LEFT JOIN 
                jobs j ON pb.job_id = j.id
            WHERE pb.job_id = '0478e109-eb41-4816-983e-a0cddd1d0ae5' AND pb.person_id = 'b9fe7aec-71e6-4a91-bd4b-ad11c3be0f5a'
            `)
        const bulkOperations = await Promise.all(bookmarksRecords.map(async (bookMark: any) => {
            const user = await db.collection("user").findOne({ email: bookMark.userEmail, importId: "DEV-45912" });
            const job = await db.collection("jobs").findOne({ title: bookMark.jobTitle, slug: bookMark.jobSlug, importId: "DEV-45912" });

            console.info(`Processing jobsTrack: ${bookMark.userEmail}`);

            return {
                importId: "DEV-45912",
                type: 2,
                jobId: job?._id || null,
                jobTitle: job?.title || null,
                userId: user?._id || null,
                createdBy: user?._id || null,
                updatedBy: user?._id ? [user?._id] : [],
                createdAt: new Date(),
                updatedAt: new Date(),
            };
        }));
        const bulkOps = bulkOperations.map(bulkOp => {
            return {
                updateOne: {
                    filter: {
                        importId: "DEV-45912",
                        jobId: bulkOp.jobId,
                        userId: bulkOp.userId
                    },
                    update: { $set: bulkOp },
                    upsert: true,
                }
            }
        });

        if (bulkOps.length > 0) {
            await db.collection("jobsTracks").bulkWrite(bulkOps);
        }
        console.info(`Successfully migrated ${bulkOps.length} JobsTrack to MongoDB`);
    } catch (error) {
        console.error("Error - convertJobsTrackTable", error);
        throw error;
    }
};

export const convertCertificateTable = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [certificatesRecords] = await mysqlConn.query(`
            SELECT
                c.*,
                p.email AS userEmail
            FROM 
                courses c
            LEFT JOIN 
                people p ON c.person_id = p.id
                
            WHERE c.id = '247799bd-5bcd-4dac-af2f-06647569b9dc'
            `)

        const bulkOperations = await Promise.all(certificatesRecords.map(async (certi: any) => {
            const user = await db.collection("user").findOne({ email: certi?.userEmail, importId: "DEV-45912" });

            console.info(`Processing certificate: ${certi.name}`);

            return {
                importId: "DEV-45912",
                title: certi?.name,
                userId: user?._id || null,
                issueOrg: certi?.authority,
                isApproved: false,
                date: certi?.period_start,
                issueAt: certi?.period_start,
                expiredAt: certi?.period_end,
                createdBy: user?._id || null,
                updatedBy: user?._id ? [user?._id] : [],
                createdAt: certi?.created_at,
                updatedAt: certi?.updated_at,
            };
        }));
        const bulkOps = bulkOperations.map(bulkOp => {
            return {
                updateOne: {
                    filter: {
                        importId: "DEV-45912",
                        userId: bulkOp.userId,
                        title: bulkOp.title,
                    },
                    update: { $set: bulkOp },
                    upsert: true,
                }
            }
        });

        if (bulkOps.length > 0) {
            await db.collection("certificates").bulkWrite(bulkOps);
        }
        console.info(`Successfully migrated ${bulkOps.length} Certificates to MongoDB`);
    } catch (error) {
        console.error("Error - convertCertificateTable", error);
        throw error;
    }
};

export const calculatePercentageOfUsers = async (mysqlConn: any, mongoClient: any) => {
    try {
        const db = mongoClient.db(database);
        const [usersRecords] = await mysqlConn.query(`
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
            LEFT JOIN people p ON u.email = p.email;
            `);

        await Promise.all(usersRecords.map(async (user: any) => {
            const userD = await db.collection("user").findOne({ email: user?.email, importId: "DEV-45912" });
            if (!userD) return;

            const countProfilePercentage = await migrateProfilePercentage(user, db);
            const result = {
                profileCompleted: countProfilePercentage.percentage || 10,
                percentObj: countProfilePercentage.percentObj || {
                    basicDetails: 1,
                    personalDetails: 0,
                    industryDetails: 0,
                    educations: 0,
                    projects: 0,
                    experiences: 0,
                    certificates: 0,
                    accomplishments: 0,
                },
            }
            return await db.collection("user").findOneAndUpdate({ _id: userD._id }, { $set: { profileCompleted: result.profileCompleted, percentObj: result.percentObj } }, { new: true })
        }));
        console.info(`Successfully updated users profile percentage to DB`);
        return true;
    } catch (error) {
        console.error("Error - calculatePercentageOfUsers", error);
        throw error;
    }
};