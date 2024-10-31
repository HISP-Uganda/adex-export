const axios = require("axios");
const util = require("util");
const fs = require("fs");
const dotenv = require("dotenv");
const stream = require("stream");
const { promisify } = require("util");

dotenv.config();

const pipeline = promisify(stream.pipeline);
const readFile = util.promisify(fs.readFile);
const unlink = util.promisify(fs.unlink);

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

// Create axios instances for source and destination
const sourceApi = axios.create({
    baseURL: sourceConfig.baseUrl,
    auth: {
        username: sourceConfig.username,
        password: sourceConfig.password,
    },
});

const destApi = axios.create({
    baseURL: destConfig.baseUrl,
    auth: {
        username: destConfig.username,
        password: destConfig.password,
    },
});

// Function to get organization units by level from source DHIS2 instance
async function getOrgUnitsByLevel(level) {
    try {
        const response = await sourceApi.get("/api/organisationUnits.json", {
            params: {
                level: level,
                fields: "id,name",
                paging: false,
            },
        });
        return response.data.organisationUnits;
    } catch (error) {
        console.error("Error fetching organization units:", error.message);
        return [];
    }
}

// Function to download CSV data from source DHIS2 instance
async function downloadCsvFromSource(dataSetId, orgUnitId, startDate, endDate) {
    const fileName = `data_${dataSetId}_${orgUnitId}_${startDate}_${endDate}.csv`;
    const writer = fs.createWriteStream(fileName);

    try {
        const response = await sourceApi.get(
            `/api/dataValueSets.csv?dataSet=${dataSetId}&orgUnit=${orgUnitId}&startDate=${startDate}&endDate=${endDate}`,
            {
                responseType: "stream",
            },
        );

        await pipeline(response.data, writer);
        console.log(`CSV file downloaded: ${fileName}`);
        return fileName;
    } catch (error) {
        console.error("Error downloading CSV:", error.message);
        throw error;
    }
}

// Function to upload CSV data to destination DHIS2 instance
async function uploadCsvToDestination(fileName) {
    try {
        const fileContent = await readFile(fileName, "utf8");

        const response = await destApi.post("/api/dataValueSets", fileContent, {
            headers: {
                "Content-Type": "application/csv",
            },
            params: {
                dataElementIdScheme: "UID",
                orgUnitIdScheme: "UID",
                dryRun: false,
                strategy: "NEW_AND_UPDATES",
                skipAudit: true,
                async: true,
            },
        });
        console.log("CSV upload response:", response.data);
    } catch (error) {
        console.error("Error uploading CSV:", error.message);
        if (error.response) {
            console.error("Response status:", error.response.status);
            console.error("Response data:", error.response.data);
        }
        throw error;
    }
}

// Function to transfer data for a single organization unit
async function transferDataForOrgUnit(dataSetId, orgUnit, startDate, endDate) {
    try {
        console.log(`Processing org unit: ${orgUnit.name} (${orgUnit.id})`);
        const fileName = await downloadCsvFromSource(
            dataSetId,
            orgUnit.id,
            startDate,
            endDate,
        );
        console.log(`Downloaded CSV file: ${fileName}`);

        // Log file contents (first few lines)
        const fileContent = await readFile(fileName, "utf8");
        const firstFewLines = fileContent.split("\n");
        console.log("total:", firstFewLines.length);

        await uploadCsvToDestination(fileName);
        console.log(
            `Successfully uploaded data for org unit: ${orgUnit.name} (${orgUnit.id})`,
        );

        // Clean up: delete the temporary CSV file
        await unlink(fileName);
        console.log(`Deleted temporary file: ${fileName}`);
    } catch (error) {
        console.error(
            `Error processing org unit ${orgUnit.name}:`,
            error.message,
        );
    }
}

// Main function to transfer data for all org units at a specific level
async function transferDataForLevel(dataSetId, level, startDate, endDate) {
    const orgUnits = await getOrgUnitsByLevel(level);
    console.log(
        `Found ${orgUnits.length} organization units at level ${level}`,
    );

    for (const orgUnit of orgUnits) {
        await transferDataForOrgUnit(dataSetId, orgUnit, startDate, endDate);
    }
}

// Example usage
const dataSetId =
    "onFoQ4ko74y&dataSet=RtEYsASU7PG&dataSet=ic1BSWhGOso&dataSet=nGkMm2VBT4G&dataSet=VDhwrW9DiC1&dataSet=quMWqLxzcfO&dataSet=dFRD2A5fdvn&dataSet=DFMoIONIalm&dataSet=EBqVAQRmiPm"; // Replace with actual dataSet ID
const level = 5; // Replace with the desired organization unit level
const startDate = "2024-01-01";
const endDate = "2024-09-30";

transferDataForLevel(dataSetId, level, startDate, endDate).then(() => {
    console.log("Data transfer process completed for all organization units");
});
