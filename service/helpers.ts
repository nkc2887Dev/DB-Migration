const ACTIONS = {
    BASIC_DETAILS: "basicDetails",
    PERSONAL_DETAILS: "personalDetails",
    INDUSTRY_DETAILS: "industryDetails",
    EDUCATIONS: "educations",
    PROJECTS: "projects",
    EXPERIENCES: "experiences",
    ORBIT_CERTIFICATES: "orbitCertificates",
    CERTIFICATES: "certificates",
    ACCOMPLISHMENTS: "accomplishments",
};

const idGenerator = async (type: string, db: any) => {
    let series = await db.collection("seriesGenerator").findOneAndUpdate(
        { type: type },
        { $inc: { startFrom: 1, totalEntry: 1 } },
        { new: true }
    );

    return series.prefix + series.startFrom.toString() + series.suffix;
};

export const assignDefaultLicence = async (userId: string, db: any) => {
    const CandidateMaster = await db.collection("master").findOne({ code: "CANDIDATE" });

    let subscriptionData;
    subscriptionData = await db.collection("subscriptions").findOne({
        typeId: CandidateMaster._id || null,
        isDefault: true,
    });

    if (!subscriptionData) {
        return null;
    }

    const currency = await db.collection("settings").findOne({ type: "MISCELLANEOUS" });

    let orderData = {
        importId: "DEV-45912",
        subscriptionId: subscriptionData._id,
        jobLimit: subscriptionData.jobLimit,
        jobUnlimited: subscriptionData.jobUnlimited,
        subTypeNm: "CANDIDATE",
        isActive: true,
        expireAt: new Date().setMonth(new Date().getMonth() + 1),
        orderNo: await idGenerator("1", db),
        sts: "SUCCESS",
        userId: userId,
        currency: currency?.currencyType?.label?.en || "INR",
    };

    await db.collection("order").updateMany(
        { subTypeNm: "CANDIDATE", userId: userId },
        { $set: { isActive: false, importId: "DEV-45912" } }
    );

    await db.collection("order").insertOne(orderData);

    return { licenceId: subscriptionData._id, jobLimit: subscriptionData.jobLimit, jobApplicationLimit: subscriptionData.jobLimit };
};


export const migrateProfilePercentage = async (user: any, db: any) => {
    try {
        let findSetting = await db.collection("settings").findOne({ type: "PROFILE_PERCENT" });
        let findEducations = await db.collection("educations").find({ userId: user._id });
        let findAccomplishments = await db.collection("accomplishments").find({ userId: user._id });
        let findProjects = await db.collection("projects").find({ userId: user._id });
        let findExperiences = await db.collection("experiences").find({ userId: user._id });
        let findCertificates = await db.collection("certificates").find({ userId: user._id });

        let percentObj = {
            [ACTIONS.BASIC_DETAILS]: user.email || user.mobNo || user.countryCode || user.name ? 1 : 0,
            [ACTIONS.PERSONAL_DETAILS]: user.genderId && user.dob && user.cityId && user.stateId ? 1 : 0,
            [ACTIONS.INDUSTRY_DETAILS]: user.domain?.length && user.experienceIds && user.ind?.length && user.skillIds?.length ? 1 : 0,
            [ACTIONS.EDUCATIONS]: findEducations.length || 0,
            [ACTIONS.PROJECTS]: findProjects.length || 0,
            [ACTIONS.EXPERIENCES]: findExperiences.length || 0,
            [ACTIONS.CERTIFICATES]: findCertificates.length || 0,
            [ACTIONS.ACCOMPLISHMENTS]: findAccomplishments.length || 0
        }
        let percentage = 0;
        for (let [key, value] of Object.entries(percentObj)) {
            if (value) {
                percentage += findSetting.settings[0][key]
            }
        }
        return { percentage, percentObj };
    } catch (error) {
        console.error("Error - migrateProfilePercentage", error);
        throw error;
    }
};

export const slugify = (text: any) => {
    return text?.toString()
        .toLowerCase()
        .replace(/\s+/g, "-") // Replace spaces with -
        .replace(/[^\w\-]+/g, "") // Remove all non-word chars
        .replace(/\-\-+/g, "-") // Replace multiple - with single -
        .replace(/^-+/, "") // Trim - from start of text
        .replace(/-+$/, ""); // Trim - from end of text
};

export const createCode = async (str: string) => {
    return str.replace(/[^\s\w]/gi, " ").toUpperCase().replace(/ /g, "_").toString()
}

export const findOrCreateMaster = async (masterNm: any, parentCode: any, db: any) => {
    try {
        const Master = await db.collection("master");
        const masterCode = await createCode(masterNm)
        const parentMaster = parentCode ? await Master.findOne({ code: parentCode }) : null;
        const parentId = parentMaster?._id || null;
        await Master.findOneAndUpdate(
            { code: masterCode },
            {
                $set: {
                    importId: "DEV-45912",
                    name: masterNm,
                    code: masterCode,
                    parentId,
                    parentCode,
                    names: { en: masterNm, id: masterNm }
                }
            },
            { new: true, upsert: true }
        );
        const master = await Master.findOne({ code: masterCode });
        return { _id: master._id, names: master.names };
    } catch (error) {
        console.error("Error-findOrCreateMaster", error)
        return false
    }
}

export const updateUserQualificationsWithDetails = async (educationRecords: any, db: any) => {
    try {
        const userQualifications = await Promise.all(educationRecords.map(async (education: any) => {
            const educations = await db.collection("educations").find({ importId: 'DEV-45912', userId: education.userId }).toArray();

            const qualifications = await educations.map((education: any) => ({
                _id: education._id,
                qualificationId: education.qualificationId,
                qualificationNm: education.qualificationNm,
                fieldOfStuNm: education.fieldOfStuNm,
            }));
            await db.collection("user").findOneAndUpdate({ _id: education.userId }, { $set: { qualifications } }, { new: true });
            console.info(`Processing update user with qualification details: ${education.institutionId}`);
        }));

        console.info(`Successfully updated ${userQualifications.length} users with qualifications.`);
    } catch (error) {
        console.error("Error - updateUserQualificationsWithDetails", error);
        throw error;
    }
};

export const updateUserExperienceWithDetails = async (experiencesRecords: any, db: any) => {
    try {
        const userExperience = await Promise.all(experiencesRecords.map(async (experience: any) => {
            const experiences = await db.collection("experiences").find({ importId: 'DEV-45912', userId: experience.userId }).toArray();

            const experienceIds = await experiences.map((experience: any) => ({
                id: experience._id,
                expNm: {
                    "en": experience.title,
                    "id": experience.title,
                },
            }));
            await db.collection("user").findOneAndUpdate({ _id: experience.userId }, { $set: { experienceIds } }, { new: true });
            console.info(`Processing update user with experience details: ${experience.title}`);
        }));

        console.info(`Successfully updated ${userExperience.length} users with experiences.`);
    } catch (error) {
        console.error("Error - updateUserExperienceWithDetails", error);
        throw error;
    }
};

export const updateUserResumeWithDetails = async (resumesRecords: any, db: any) => {
    try {
        const userResumes = await Promise.all(resumesRecords.map(async (resume: any) => {
            const resumesFromFile = await db.collection("file").find({ importId: 'DEV-45912', userId: resume.userId }).toArray();
            resumesFromFile.sort((a: any, b: any) => b.createdAt - a.createdAt);

            const resumes = await resumesFromFile.map((resume: any) => ({
                tempId: resume._id,
                uri: resume.uri,
                oriNm: resume.oriNm,
                prevUsed: false,
                createdAt: resume.createdAt,
            }));
            await db.collection("user").findOneAndUpdate({ _id: resume.userId }, { $set: { resumes, resumeHeaders: USER_TABS_HEADER, resumeFileId: resumesFromFile[0]._id } }, { new: true })
            console.info(`Processing update user with resume details: ${resume.oriNm}`);
        }));

        console.info(`Successfully updated ${userResumes.length} users with resumes.`);
    } catch (error) {
        console.error("Error - updateUserResumeWithDetails", error);
        throw error;
    }
};

export const updateUserWithCompanyDetails = async (companyRecords: any, db: any) => {
    try {
        return await Promise.all(companyRecords.map(async (company: any) => {
            const companyDetails = await db.collection("company").findOne({ importId: 'DEV-45912', compNm: company.name, slug: company.slug });
            if (!companyRecords?.userIds?.[0]) return;
            await db.collection("user").findOneAndUpdate({ _id: companyRecords.userIds[0] }, { $set: { compId: companyDetails._id, compNm: companyDetails.name } }, { new: true })
            console.info(`Processing update user with comapny details: ${company.name}`);
            return;
        }));
    } catch (error) {
        console.error("Error - updateUserResumeWithDetails", error);
        throw error;
    }
};

export const updateCompanyWithUserDetails = async (mysqlConn: any, bulkOperations: any, db: any) => {
    try {
        return await Promise.all(bulkOperations.map(async (user: any) => {
            if (!user?.compId && !user.email) return null;
            const userEmail = await db.collection("user").findOne({ email: user.email });
            const [attachmentsRecords] = await mysqlConn.query(`
                SELECT atc.*, p.email AS userEmail  
                FROM attachments atc
                JOIN users p ON atc.created_by = p.id
                WHERE p.email = ?;
            `, [user.email]);

            const attach = attachmentsRecords?.[0] || null;
            if(!attach && attachmentsRecords.length === 0) return null;
            await db.collection("file").findOneAndUpdate({ userId: userEmail._id, nm: attach.name, importId: "DEV-45912" }, {
                $set: {
                    importId: "DEV-45912",
                    userId: userEmail?._id,
                    nm: attach.name,
                    oriNm: attach.name,
                    type: attach.mime_type,
                    exten: attach.path.split('.').pop(),
                    uri: attach.path,
                    sts: 2,
                    mimeType: attach.mime_type,
                    createdAt: attach.created_at,
                    updatedAt: attach.updated_at,
                }
            }, { new: true, upsert: true });

            const logoFile = await db.collection("file").findOne({ userId: userEmail._id, nm: attach.name, importId: "DEV-45912" });
            await db.collection("company").findOneAndUpdate({ _id: user.compId }, { $set: { userIds: [userEmail._id], ...(logoFile ? { logoId: logoFile._id } : {}) } }, { new: true });
            console.info(`Processing update company with user details: ${user.email}`);
            return true;
        }));
    } catch (error) {
        console.error("Error - updateCompanyWithUserDetails", error);
        throw error;
    }
};

export const updateUserAndJobsDetails = async (bulkOperations: any, db: any) => {
    try {
        return await Promise.all(bulkOperations.map(async (userJobs: any) => {
            if (!userJobs?.jobId || !userJobs?.userId) return null;
            await db.collection("user").findOneAndUpdate({ _id: userJobs.userId }, { $inc: { 'jobs.appliedJobCount': 1 } }, { new: true });
            await db.collection("jobs").findOneAndUpdate({ _id: userJobs.jobId }, { $inc: { appliedCount: 1 } }, { new: true });
            return true;
        }));
    } catch (error) {
        console.error("Error - updateUserAndJobsDetails", error);
        throw error;
    }
};

export const parseAddress = (addressString: string) => {
    if (!addressString) return {};

    const parts = addressString.split(',').map(part => part.trim());
    return {
        street: parts[0] || undefined,
        address1: parts[1] || undefined,
        zipCode: parts[2]?.match(/\d+/)?.[0] || undefined,
        cityNm: parts[2]?.replace(/\d+/g, '').trim() || undefined,
        countryNm: parts[3] || undefined,
    };
};

export const makeItYopmail = (email: string) => {
    email = email?.split('@')[0] + '@yopmail.com';
    return email
};

export const addCompaniesToEmployer = async (user: any, mysqlConn: any, db: any) => {
    try {
        // const [mysqlCompany] = await mysqlConn.query(`SELECT * FROM companies WHERE created_by = ?;`, [user.people_id]);
        const [mysqlCompany] = await mysqlConn.query(`SELECT
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
                cm.created_at AS createdAt,
                cm.updated_at AS updatedAt,
                cm.og_image_id AS bannerId,
                cm.is_visible AS isActive,
                cm.video_url,
                cm.vr_url,
                p.email AS employerEmail,
                att.name AS logoURL,
                att.created_by AS attachCreateBy,
                    JSON_OBJECT(
                    'personId', pc.person_id,
                    'companyId', pc.company_id,
                    'role', pc.role
                ) AS personCompanies
            FROM companies cm
            LEFT JOIN people p ON cm.created_by = p.id
            LEFT JOIN person_companies pc ON cm.created_by = pc.person_id
            LEFT JOIN attachments att ON cm.logo_id = att.id
            WHERE cm.created_by = ?;
        `, [user.people_id]);

        if (mysqlCompany && mysqlCompany.length > 0) {
            let { name, slug, nm, logoURL, employerEmail, logoId, address, licenceNo, isActive, compWebURL, compLinkedInURL, profilePercent, benefits, compDomains, createdAt, updatedAt } = mysqlCompany[0];

            const parsedAddress = parseAddress(address);
            await db.collection("company").findOneAndUpdate(
                { compNm: name, slug: slug, importId: "DEV-45912" },
                {
                    $set: {
                        importId: "DEV-45912",
                        isActive: !!isActive,
                        isDefault: false,
                        compNm: name,
                        slug: slug,
                        conPer: {
                            name: nm || user.name,
                            mobileNo: user.phone,
                            email: user.email
                        },
                        userIds: [],
                        licenceNo: licenceNo || undefined,

                        compWebURL: compWebURL,
                        compLinkedInURL: compLinkedInURL,
                        profilePercent: profilePercent || 0,
                        address: {
                            street: parsedAddress.street,
                            address1: parsedAddress.address1,
                            countryNm: parsedAddress.countryNm,
                            cityNm: parsedAddress.cityNm,
                            zipCode: parsedAddress.zipCode,
                        },
                        benefits: benefits || undefined,
                        compDomains: compDomains || undefined,
                        createdBy: user?._id || null,
                        createdAt: createdAt,
                        updatedAt: updatedAt,
                    }
                },
                {
                    new: true,
                    upsert: true,
                }
            );
            return await db.collection("company").findOne({ compNm: name, slug: slug, importId: "DEV-45912" });
        } else {
            await db.collection("company").findOneAndUpdate(
                { compNm: user.name },
                {
                    $set: {
                        importId: "DEV-45912",
                        isActive: true,
                        isDefault: false,
                        compNm: user.name,
                        slug: slugify(user.name),
                        conPer: {
                            name: user.name,
                            mobileNo: user.phone,
                            email: user.email
                        },
                        userIds: [],
                        createdAt: new Date(),
                        updatedAt: new Date()
                    }
                }, {
                new: true,
                upsert: true
            });
            return await db.collection("company").findOne({ compNm: user.name, importId: "DEV-45912" });
        }
    } catch (error) {
        console.error("Error - addCompaniesToEmployer", error);
        throw error;
    }
};