import { MongoClient } from "mongodb";

export const connectionOfMongo = async () => {
    try {

      const mongoClient = new MongoClient(process.env.MONGO_URL as any);
      await mongoClient.connect();
      
      console.info("Connected to MongoDB successfully");
      return mongoClient;
    } catch (error: any) {
      console.error("Error connecting to MongoDB:", error.message);
      throw error;
    }
};