const mongoose = require("mongoose");

export const connectionOfMongo = async () => {
    try {
      await mongoose.connect(process.env.MONGO_URL, {
        connectTimeoutMS: 10000,
      });
      console.log("Connected to MongoDB successfully");
    } catch (error: any) {
      console.error("Error connecting to MongoDB:", error.message);
      throw error;
    }
};