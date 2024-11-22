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
        if (masterNm) {
            const masterCode = await createCode(masterNm)
            const parentMaster = await Master.findOne({ code: parentCode })
            let master = parentMaster && await Master.findOneAndUpdate(
                { code: masterCode },
                {
                    $set: {
                        importId: "DEV-45912",
                        name: masterNm,
                        code: masterCode,
                        parentId: parentMaster._id,
                        parentCode: parentCode,
                        names: { en: masterNm, id: masterNm }
                    }
                },
                { new: true, upsert: true }
            );
            return ({ _id: master._id, names: master.names })
        }
        return false;
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
            await db.collection("user").findOneAndUpdate({ _id: education.userId }, { $set: { qualifications } }, { new: true })
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
                    "en":experience.title,
                    "id":experience.title,
                },
            }));
            await db.collection("user").findOneAndUpdate({ _id: experience.userId }, { $set: { experienceIds } }, { new: true })
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
            await db.collection("user").findOneAndUpdate({ _id: resume.userId }, { $set: { resumes, resumeHeaders: process.env.USER_TABS_HEADER, resumeFileId: resumesFromFile[0]._id } }, { new: true })
        }));

        console.info(`Successfully updated ${userResumes.length} users with resumes.`);
    } catch (error) {
        console.error("Error - updateUserResumeWithDetails", error);
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
