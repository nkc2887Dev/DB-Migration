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

const createCode = async (str: string) => {
    return str.replace(/[^\s\w]/gi, " ").toUpperCase().replace(/ /g, "_").toString()
}

export const findOrCreateMaster = async (masterNm:any, parentCode:any, db:any) => {
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
        return false
    } catch (error) {
        console.error("Error-findOrCreateMaster", error)
        return false
    }
}