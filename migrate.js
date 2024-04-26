import fs from 'fs';
import path from 'path';
import { pipeline, Transform } from 'stream';
import { promisify } from 'util';
import csv from 'csv-parser';
import { stringify } from 'csv-stringify';

const weatherStationData = {
    NUM_POSTE: "stationNumber",
    NOM_USUEL: "stationName",
    LAT: "latitude",
    LON: "longitude",
    ALTI: "altitude",
    AAAAMMJJ: "date",
    RR: "precipitation",
    QRR: "qualityPrecipitation",
    TN: "minTemperature",
    QTN: "qualityMinTemperature",
    HTN: "minTempTime",
    QHTN: "qualityMinTempTime",
    TX: "maxTemperature",
    QTX: "qualityMaxTemperature",
    HTX: "maxTempTime",
    QHTX: "qualityMaxTempTime",
    TM: "avgHourlyTemp",
    QTM: "qualityAvgHourlyTemp",
    TNTXM: "avgDailyTemp",
    QTNTXM: "qualityAvgDailyTemp",
    TAMPLI: "tempAmplitude",
    QTAMPLI: "qualityTempAmplitude",
    TNSOL: "minGroundTemp",
    QTNSOL: "qualityMinGroundTemp",
    TN50: "min50cmTemp",
    QTN50: "qualityMin50cmTemp",
    DG: "frostDuration",
    QDG: "qualityFrostDuration",
    FFM: "avgWindSpeed10m",
    QFFM: "qualityAvgWindSpeed10m",
    FF2M: "avgWindSpeed2m",
    QFF2M: "qualityAvgWindSpeed2m",
    FXY: "maxWindSpeed",
    QFXY: "qualityMaxWindSpeed",
    DXY: "maxWindDirection",
    QDXY: "qualityMaxWindDirection",
    HXY: "maxWindTime",
    QHXY: "qualityMaxWindTime",
    FXI: "maxInstantWindSpeed",
    QFXI: "qualityMaxInstantWindSpeed",
    DXI: "maxInstantWindDirection",
    QDXI: "qualityMaxInstantWindDirection",
    HXI: "maxInstantWindTime",
    QHXI: "qualityMaxInstantWindTime",
    FXI2: "maxInstantWindSpeed2m",
    QFXI2: "qualityMaxInstantWindSpeed2m",
    DXI2: "maxInstantWindDirection2m",
    QDXI2: "qualityMaxInstantWindDirection2m",
    HXI2: "maxInstantWindTime2m",
    QHXI2: "qualityMaxInstantWindTime2m",
    FXI3S: "max3sWindSpeed",
    QFXI3S: "qualityMax3sWindSpeed",
    DXI3S: "max3sWindDirection",
    QDXI3S: "qualityMax3sWindDirection",
    HXI3S: "max3sWindTime",
    QHXI3S: "qualityMax3sWindTime",
    DHUMEC: "wetDuration",
    QDHUMEC: "qualityWetDuration",
    PMERM: "avgSeaLevelPressure",
    QPMERM: "qualityAvgSeaLevelPressure",
    PMERMIN: "minSeaLevelPressure",
    QPMERMIN: "qualityMinSeaLevelPressure",
    INST: "sunshineDuration",
    QINST: "qualitySunshineDuration",
    GLOT: "globalRadiation",
    QGLOT: "qualityGlobalRadiation",
    DIFT: "diffuseRadiation",
    QDIFT: "qualityDiffuseRadiation",
    DIRT: "directRadiation",
    QDIRT: "qualityDirectRadiation",
    INFRART: "infraredRadiation",
    QINFRART: "qualityInfraredRadiation",
    UV: "uvRadiation",
    QUV: "qualityUvRadiation",
    UV_INDICEX: "maxUvIndex",
    QUV_INDICEX: "qualityMaxUvIndex",
    SIGMA: "sunshineFraction",
    QSIGMA: "qualitySunshineFraction",
    UN: "minHumidity",
    QUN: "qualityMinHumidity",
    HUN: "minHumidityTime",
    QHUN: "qualityMinHumidityTime",
    UX: "maxHumidity",
    QUX: "qualityMaxHumidity",
    HUX: "maxHumidityTime",
    QHUX: "qualityMaxHumidityTime",
    UM: "avgHumidity",
    QUM: "qualityAvgHumidity",
    DHUMI40: "humidityBelow40Duration",
    QDHUMI40: "qualityHumidityBelow40Duration",
    DHUMI80: "humidityAbove80Duration",
    QDHUMI80: "qualityHumidityAbove80Duration",
    TSVM: "vaporPressure",
    QTSVM: "qualityVaporPressure",
    ETPMON: "etpMonteith",
    QETPMON: "qualityEtpMonteith",
    ETPGRILLE: "etpGridPoint",
    QETPGRILLE: "qualityEtpGridPoint",
    ECOULEMENTM: "flowLevel",
    QECOULEMENTM: "qualityFlowLevel",
    HNEIGEF: "freshSnowHeight",
    QHNEIGEF: "qualityFreshSnowHeight",
    NEIGETOTX: "maxSnowDepth",
    QNEIGETOTX: "qualityMaxSnowDepth",
    NEIGETOT06: "totalSnowDepth6h",
    QNEIGETOT06: "qualityTotalSnowDepth6h",
    NEIG: "snowOccurrence",
    QNEIG: "qualitySnowOccurrence",
    BROU: "fogOccurrence",
    QBROU: "qualityFogOccurrence",
    ORAG: "thunderstormOccurrence",
    QORAG: "qualityThunderstormOccurrence",
    GRESIL: "sleetOccurrence",
    QGRESIL: "qualitySleetOccurrence",
    GRELE: "hailOccurrence",
    QGRELE: "qualityHailOccurrence",
    ROSEE: "dewOccurrence",
    QROSEE: "qualityDewOccurrence",
    VERGLAS: "iceOccurrence",
    QVERGLAS: "qualityIceOccurrence",
    SOLNEIGE: "snowCoverOccurrence",
    QSOLNEIGE: "qualitySnowCoverOccurrence",
    GELEE: "frostOccurrence",
    QGELEE: "qualityFrostOccurrence",
    FUMEE: "smokeOccurrence",
    QFUMEE: "qualitySmokeOccurrence",
    BRUME: "mistOccurrence",
    QBRUME: "qualityMistOccurrence",
    ECLAIR: "lightningOccurrence",
    QECLAIR: "qualityLightningOccurrence",
    NB300: "cloudCoverageAbove4octasBelow300m",
    QNB300: "qualityCloudCoverageAbove4octasBelow300m",
    BA300: "minHeightOfNB300",
    QBA300: "qualityMinHeightOfNB300",
    TMERMIN: "minSeaWaterTemp",
    QTMERMIN: "qualityMinSeaWaterTemp",
    TMERMAX: "maxSeaWaterTemp",
    QTMERMAX: "qualityMaxSeaWaterTemp"
};

const plantHealthData = {
    NUM_POSTE: "stationNumber",
    NOM_USUEL: "stationName",
    LAT: "latitude",
    LON: "longitude",
    ALTI: "altitude",
    AAAAMMJJ: "date",
    RR: "precipitation",
    QRR: "qualityPrecipitation",
    TN: "minTemperature",
    QTN: "qualityMinTemperature",
    TX: "maxTemperature",
    QTX: "qualityMaxTemperature",
    TM: "avgHourlyTemp",
    QTM: "qualityAvgHourlyTemp",
    TNSOL: "minGroundTemp",
    QTNSOL: "qualityMinGroundTemp",
    PMERM: "avgSeaLevelPressure",
    QPMERM: "qualityAvgSeaLevelPressure",
    PMERMIN: "minSeaLevelPressure",
    QPMERMIN: "qualityMinSeaLevelPressure",
    INST: "sunshineDuration",
    QINST: "qualitySunshineDuration",
    UN: "minHumidity",
    QUN: "qualityMinHumidity",
    UX: "maxHumidity",
    QUX: "qualityMaxHumidity",
    UM: "avgHumidity",
    QUM: "qualityAvgHumidity",
    DG: "frostDuration",
    QDG: "qualityFrostDuration",
    FFM: "avgWindSpeed10m",
    QFFM: "qualityAvgWindSpeed10m",
    FF2M: "avgWindSpeed2m",
    QFF2M: "qualityAvgWindSpeed2m"
};


const pipe = promisify(pipeline);

async function getFilesWithSubstring(dirPath, substring) {
    const files = await fs.promises.readdir(dirPath);
    return files.filter(file => file.includes(substring));
}

async function processFile(inputFile, outputFile) {
    const inputStream = fs.createReadStream(inputFile);
    const outputStream = fs.createWriteStream(outputFile);
    const csvTransform = csv({ separator: ';' });

    const filterAndRenameColumns = new Transform({
        objectMode: true,
        transform(data, encoding, callback) {
            // Filter and rename columns
            const filtered = Object.keys(data)
                .filter(key => key in plantHealthData)
                .reduce((newData, key) => {
                    newData[plantHealthData[key]] = data[key];
                    return newData;
                }, {});
            this.push(filtered);
            callback();
        }
    });

    const stringifier = stringify({ header: true, delimiter: ';' });

    try {
        await pipe(
            inputStream,
            csvTransform,
            filterAndRenameColumns,
            stringifier,
            outputStream
        );
    } catch (error) {
        console.error('Error processing file:', inputFile, error);
    }
}

async function processAllFiles(dirPath, substring, outputDir) {
    const fileNames = await getFilesWithSubstring(dirPath, substring);
    for (let fileName of fileNames) {
        console.log(`Processing ${fileName}`);
        const inputPath = path.join(dirPath, fileName);
        const outputPath = path.join(outputDir, fileName);
        await processFile(inputPath, outputPath);
        console.log(`Finished processing ${fileName}`);
    }
}

// Example of using the refactored function
const dirPath = "./datasets";
const outputDir = "./filtered_datasets";
const substring = "RR-T-Vent";

processAllFiles(dirPath, substring, outputDir)
    .then(() => console.log("All files processed."))
    .catch(error => console.error("Error processing files:", error));