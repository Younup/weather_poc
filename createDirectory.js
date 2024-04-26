import fs from "fs"

async function getFilesWithSubstring(year, department) {
    const dirPath = "./filtered_datasets"
    const files = await fs.promises.readdir(dirPath);
    return files.filter(file => findFile(year, department, file));
}

function findFile(year, department, fileName) {
    const [, dataDepartment, period] = fileName.split("_")
    const [, startYear, endYear] = period.split("-");
    // If departments do not match return false
    if (department !== Number(dataDepartment)) return false;
    // If the years are not matching return false;
    if (year < Number(startYear) || Number(year) > endYear) return false;
    // If years and department match return true;
    return true;
}

// Determine department and year for line, then findFile(). then normal Matching

async function createDirectory() {
    const dirPath = "./filtered_datasets"
    const files = await fs.promises.readdir(dirPath);
    const directory = {};

    for (const fileName of files) {

        const [, department, period] = fileName.split("_")
        const [, startYear, endYear] = period.split("-");
        const item = { startYear: Number(startYear), endYear: Number(endYear), fileName }
        if (directory[department]) {
            directory[department].push(item)
        } else {
            directory[department] = [item]
        }
        console.log({ startYear, endYear, fileName, department })
    }

    fs.writeFile("fileDirectory.json", JSON.stringify(directory), (err) => {
        console.log(err)
    })
}

createDirectory()