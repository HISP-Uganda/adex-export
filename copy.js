const axios = require("axios");
const { Transform } = require("stream");
const csv = require("csv-parser");
const { pipeline } = require("stream/promises");
const EventEmitter = require("events");
const dotenv = require("dotenv");

// Increase default max listeners
EventEmitter.defaultMaxListeners = 20;

dotenv.config();

// DHIS2 instance configurations
const sourceConfig = {
    baseUrl: process.env.SOURCE_DHIS2_URL,
    username: process.env.SOURCE_DHIS2_USERNAME,
    password: process.env.SOURCE_DHIS2_PASSWORD,
};

const destConfig = {
    baseUrl: process.env.DEST_DHIS2_URL,
    username: process.env.DEST_DHIS2_USERNAME,
    password: process.env.DEST_DHIS2_PASSWORD,
};

// Create axios instances with basic auth
const sourceClient = axios.create({
    baseURL: sourceConfig.baseUrl,
    auth: {
        username: sourceConfig.username,
        password: sourceConfig.password,
    },
});

const sourceStreamClient = axios.create({
    baseURL: sourceConfig.baseUrl,
    auth: {
        username: sourceConfig.username,
        password: sourceConfig.password,
    },
    responseType: "stream",
});

const destClient = axios.create({
    baseURL: destConfig.baseUrl,
    auth: {
        username: destConfig.username,
        password: destConfig.password,
    },
});

// Helper function to validate date format (YYYY-MM-DD)
function isValidDate(dateString) {
    const regex = /^\d{4}-\d{2}-\d{2}$/;
    if (!regex.test(dateString)) return false;

    const date = new Date(dateString);
    return date instanceof Date && !isNaN(date);
}

// Helper function to format date for DHIS2 API
function formatDateForDHIS2(date) {
    return date.replace(/-/g, "");
}

// Transform stream to convert CSV data to DHIS2 API compatible format
class PayloadTransformer extends Transform {
    constructor(options = {}) {
        super({ ...options, objectMode: true });
    }

    _transform(chunk, encoding, callback) {
        const payload = {
            dataElement: chunk.dataElement,
            period: chunk.period,
            orgUnit: chunk.orgUnit,
            value: chunk.value,
            categoryOptionCombo: chunk.categoryOptionCombo,
            attributeOptionCombo: chunk.attributeOptionCombo,
        };
        callback(null, payload);
    }

    _destroy() {
        this.removeAllListeners();
    }
}

// Batch processor to handle multiple records
class BatchProcessor extends Transform {
    constructor(options = {}) {
        super({ ...options, objectMode: true });
        this.batch = [];
        this.batchSize = options.batchSize || 100;
    }

    _transform(chunk, encoding, callback) {
        this.batch.push(chunk);

        if (this.batch.length >= this.batchSize) {
            this.push(this.batch);
            this.batch = [];
        }

        callback();
    }

    _flush(callback) {
        if (this.batch.length > 0) {
            this.push(this.batch);
        }
        callback();
    }

    _destroy() {
        this.batch = [];
        this.removeAllListeners();
    }
}

// Data uploader transform stream
class DataUploader extends Transform {
    constructor(destClient, options = {}) {
        super({ ...options, objectMode: true });
        this.destClient = destClient;
        this.recordCount = 0;
        this.batchCount = 0;
        this.startTime = Date.now();
        this.orgUnit = options.orgUnit;
    }

    async _transform(batch, encoding, callback) {
        try {
            await this.destClient.post("/api/dataValueSets", {
                dataValues: batch,
            });
            this.recordCount += batch.length;
            this.batchCount++;

            const elapsedSeconds = (Date.now() - this.startTime) / 1000;
            const recordsPerSecond = this.recordCount / elapsedSeconds;

            console.log(
                `[${this.orgUnit.name}] Batch ${this.batchCount}: Imported ${
                    this.recordCount
                } records (${recordsPerSecond.toFixed(2)} records/sec)`,
            );
            callback();
        } catch (error) {
            console.error(
                `[${this.orgUnit.name}] Error importing batch:`,
                error.message,
            );
            if (error.response) {
                console.error("Error details:", error.response.data);
            }
            callback(error);
        }
    }

    _destroy() {
        this.removeAllListeners();
    }
}

// Fetch organization units for multiple levels
async function getOrgUnitsMultiLevel(levels) {
    try {
        const filter = `level:in:[${levels.join(",")}]`;

        const response = await sourceClient.get("/api/organisationUnits", {
            params: {
                fields: "id,name,level,parent[id,name]",
                filter: filter,
                paging: false,
            },
        });

        const orgUnits = response.data.organisationUnits;

        const orgUnitsByLevel = orgUnits.reduce((acc, ou) => {
            acc[ou.level] = acc[ou.level] || [];
            acc[ou.level].push(ou);
            return acc;
        }, {});

        Object.entries(orgUnitsByLevel).forEach(([level, units]) => {
            console.log(
                `Found ${units.length} organization units at level ${level}`,
            );
        });

        return orgUnits;
    } catch (error) {
        console.error("Error fetching org units:", error.message);
        throw error;
    }
}

// Process single organization unit
async function processOrgUnit(
    orgUnit,
    dataSets,
    startDate,
    endDate,
    batchSize,
) {
    let streams = [];

    try {
        const dataSetParams = dataSets.map((ds) => `dataSet=${ds}`).join("&");
        const startDateFormatted = formatDateForDHIS2(startDate);
        const endDateFormatted = formatDateForDHIS2(endDate);

        console.log(`\nProcessing ${orgUnit.name} (Level ${orgUnit.level})`);
        console.log(`Date range: ${startDate} to ${endDate}`);

        const url = `/api/dataValueSets.csv?${dataSetParams}&orgUnit=${orgUnit.id}&startDate=${startDateFormatted}&endDate=${endDateFormatted}`;
        const response = await sourceStreamClient.get(url);

        const payloadTransformer = new PayloadTransformer();
        const batchProcessor = new BatchProcessor({ batchSize });
        const dataUploader = new DataUploader(destClient, { orgUnit });

        streams = [
            response.data,
            payloadTransformer,
            batchProcessor,
            dataUploader,
        ];

        await pipeline(
            response.data,
            csv(),
            payloadTransformer,
            batchProcessor,
            dataUploader,
        );

        return {
            orgUnitId: orgUnit.id,
            orgUnitName: orgUnit.name,
            recordCount: dataUploader.recordCount,
            batchCount: dataUploader.batchCount,
        };
    } catch (error) {
        console.error(
            `Error processing org unit ${orgUnit.name}:`,
            error.message,
        );
        return {
            orgUnitId: orgUnit.id,
            orgUnitName: orgUnit.name,
            error: error.message,
        };
    } finally {
        streams.forEach((stream) => {
            if (stream && typeof stream.destroy === "function") {
                stream.destroy();
            }
        });
    }
}

// Process organization units concurrently
async function processConcurrentOrgUnits(
    orgUnits,
    dataSets,
    startDate,
    endDate,
    batchSize,
    concurrency = 3,
) {
    const results = [];
    const queue = [...orgUnits];
    const active = new Set();
    let completed = 0;

    console.log(`Starting transfer with concurrency of ${concurrency}`);
    console.log(`Date range: ${startDate} to ${endDate}`);

    const startTime = Date.now();

    while (queue.length > 0 || active.size > 0) {
        while (queue.length > 0 && active.size < concurrency) {
            const orgUnit = queue.shift();
            const promise = processOrgUnit(
                orgUnit,
                dataSets,
                startDate,
                endDate,
                batchSize,
            ).then((result) => {
                active.delete(promise);
                completed++;

                const progress = ((completed / orgUnits.length) * 100).toFixed(
                    1,
                );
                const elapsedMinutes = (
                    (Date.now() - startTime) /
                    1000 /
                    60
                ).toFixed(1);

                console.log(
                    `\nProgress: ${completed}/${orgUnits.length} (${progress}%) - ${elapsedMinutes} minutes elapsed`,
                );

                return result;
            });

            active.add(promise);
            results.push(promise);
        }

        if (active.size >= concurrency || queue.length === 0) {
            await Promise.race(active);
        }
    }

    return await Promise.all(results);
}

// Main transfer function
async function transferData(
    dataSets,
    startDate,
    endDate,
    orgUnitLevels,
    batchSize = 100,
    concurrency = 3,
) {
    try {
        // Validate dates
        if (!isValidDate(startDate) || !isValidDate(endDate)) {
            throw new Error(
                "Invalid date format. Please use YYYY-MM-DD format",
            );
        }

        const start = new Date(startDate);
        const end = new Date(endDate);
        if (start > end) {
            throw new Error("Start date must be before or equal to end date");
        }

        console.log("Fetching organization units...");
        const orgUnits = await getOrgUnitsMultiLevel(orgUnitLevels);
        console.log(`Found total ${orgUnits.length} organization units`);
        console.log(`Date range: ${startDate} to ${endDate}`);

        const startTime = Date.now();

        const results = await processConcurrentOrgUnits(
            orgUnits,
            dataSets,
            startDate,
            endDate,
            batchSize,
            concurrency,
        );

        // Generate summary
        const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);
        const successful = results.filter((r) => !r.error);
        const failed = results.filter((r) => r.error);

        const totalRecords = successful.reduce(
            (sum, r) => sum + r.recordCount,
            0,
        );

        console.log("\n=== Transfer Summary ===");
        console.log(`Date Range: ${startDate} to ${endDate}`);
        console.log(`Total time: ${totalTime} seconds`);
        console.log(`Successfully processed: ${successful.length} org units`);
        console.log(`Failed: ${failed.length} org units`);
        console.log(`Total records transferred: ${totalRecords}`);

        if (failed.length > 0) {
            console.log("\nFailed organization units:");
            failed.forEach((f) => {
                console.log(`- ${f.orgUnitName}: ${f.error}`);
            });
        }
    } catch (error) {
        console.error("Error in data transfer process:", error);
        throw error;
    }
}

// Example usage
const config = {
    dataSets: [
        "onFoQ4ko74y",
        "RtEYsASU7PG",
        "ic1BSWhGOso",
        "nGkMm2VBT4G",
        "VDhwrW9DiC1",
        "quMWqLxzcfO",
        "dFRD2A5fdvn",
        "DFMoIONIalm",
        "EBqVAQRmiPm",
    ], // Replace with your dataset IDs
    startDate: "2024-01-01", // Format: YYYY-MM-DD
    endDate: "2024-10-31", // Format: YYYY-MM-DD
    orgUnitLevels: [5, 6],
    batchSize: 100,
    concurrency: 3, // Number of concurrent org unit transfers
};

// Run the transfer
transferData(
    config.dataSets,
    config.startDate,
    config.endDate,
    config.orgUnitLevels,
    config.batchSize,
    config.concurrency,
).catch(console.error);

// Process handling
process.on("unhandledRejection", (error) => {
    console.error("Unhandled rejection:", error);
    process.exit(1);
});

process.on("SIGTERM", () => {
    console.log("Received SIGTERM. Cleaning up...");
    process.exit(0);
});

process.on("SIGINT", () => {
    console.log("Received SIGINT. Cleaning up...");
    process.exit(0);
});
