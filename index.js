import axios from "axios";
import fs from "fs"
import { createGunzip } from "zlib";
import path from "path";
import fs from 'fs';
import path from 'path';
import { pipeline } from 'stream';
import { promisify } from 'util';
import csv from 'csv-parser';  // Use a CSV parser to handle the CSV file transformation
import csvStringify from 'csv-stringify';  // For 

async function downloadZip(url, outputFolder, downloadFile) {
    return new Promise(async (res, rej) => {
        const response = await axios({
            url,
            method: 'GET',
            responseType: 'arraybuffer'
        });
        const buffer = Buffer.from(response.data, 'binary');
        fs.writeFile(`${outputFolder}/${downloadFile}`, buffer, () => {
            res(true)
        });
    })

};

const extractZip = (filePath, outputFolder) => {
    return new Promise((res, rej) => {
        // Replace 'input.gz' with the path to your .gz file
        // Replace 'output' with the path where you want to save the decompressed file
        const readStream = fs.createReadStream(filePath);
        const writeStream = fs.createWriteStream(outputFolder);
        const unzip = createGunzip();

        readStream.pipe(unzip).pipe(writeStream).on('finish', () => {
            res('File decompressed successfully');
        });
    })
};

export async function downloadAndUnzipFile(zipUrl, name) {
    const outputFolder = `dataSets`;
    const downloadFile = name
    try {
        console.log("Downloading ", name)
        const res = await downloadZip(zipUrl, outputFolder, downloadFile);
        console.log("Extracting ", name)
        const res1 = await extractZip(`${outputFolder}/${downloadFile}`, `${outputFolder}/${downloadFile.replace(".gz", "")}`);
        return true;
    } catch (e) {
        console.log(e);
        return false;
    }

};
const url = "https://www.data.gouv.fr/api/2/datasets/6569b51ae64326786e4e8e1a/resources/?page=1&type=main&page_size=700&q=";

const filters = [
    '1950-2022',
    '2023-2024'
]

async function removeGzFiles() {
    const directoryPath = './datasets';
    const fileExtension = '.gz';  // Change '.txt' to whatever file type you need to delete

    fs.readdir(directoryPath, (err, files) => {
        if (err) {
            console.error('Error reading directory:', err);
            return;
        }

        files.filter(file => path.extname(file) === fileExtension)
            .forEach(file => {
                const filePath = path.join(directoryPath, file);
                fs.unlink(filePath, err => {
                    if (err) {
                        console.error('Error deleting file:', filePath, err);
                    } else {
                        console.log('Deleted file:', filePath);
                    }
                });
            });
    });
}

async function getDatasets() {
    fetch(url).then(async (it) => await it.json()).then(async ({ data }) => {
        const filteredData = data.filter(({ title }) => filters.some(filter => title.includes(filter)));

        let count = 0;
        for (const { url } of filteredData) {
            const name = url.split("/")[url.split("/").length - 1];
            console.log({ name, count, total: filteredData.length })
            await downloadAndUnzipFile(url, name);
            count++;
        }

    })
}