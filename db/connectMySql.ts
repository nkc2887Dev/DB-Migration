import mysql from 'mysql2/promise';

export const connectionOfMySQL = async () => {
  try {
    const connection = await mysql.createConnection({
        host: process.env.MYSQL_HOST,
        user: process.env.MYSQL_USER,
        password: process.env.MYSQL_PASSWORD,
        database: process.env.MYSQL_DB,
        port: Number(process.env.MYSQL_PORT),
    });
    console.log('Connected to MySQL database successfully');
    return connection;
  } catch (error:any) {
    console.error('Error connecting to MySQL database:', error.message);
    throw error; 
  }
};