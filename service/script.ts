import path from "path";
import fs from "fs";

export const splitFileName = (fileName: string) => {
    return fileName.split('_')[0];
};

export const renameFilesInFolder = (folderPath: string) => {
    if (!fs.existsSync(folderPath)) {
        console.error("The specified folder does not exist.");
        return;
    }

    fs.readdir(folderPath, (err: any, files: any[]) => {
        if (err) {
            console.error("Error reading the folder:", err);
            return;
        }

        files.forEach((file) => {
            const oldFilePath = path.join(folderPath, file);
            const fileStats = fs.statSync(oldFilePath);

            if (fileStats.isFile()) {
                const ext = path.extname(file);
                const baseName = path.basename(file, ext);
                const slugifiedName = splitFileName(baseName) + ext;

                const newFilePath = path.join(folderPath, slugifiedName);

                if (oldFilePath !== newFilePath) {
                    fs.rename(oldFilePath, newFilePath, (renameErr: any) => {
                        if (renameErr) {
                            console.error(`Error renaming file ${file}:`, renameErr);
                        } else {
                            console.info(`Renamed: ${file} -> ${slugifiedName}`);
                        }
                    });
                }
            }
        });
    });
};


