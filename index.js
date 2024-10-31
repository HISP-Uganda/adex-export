const axios = require("axios");
const dotenv = require("dotenv");
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

// Function to get all organization units from destination instance
async function getDestinationOrgUnits() {
    try {
        const response = await destApi.get(
            "/api/organisationUnits.json?fields=id,name&paging=false",
        );
        return response.data.organisationUnits;
    } catch (error) {
        console.error("Error fetching destination org units:", error.message);
        return [];
    }
}

// Function to get data for a specific org unit and dataset from source instance
async function getSourceData(orgUnitId, dataSetId, startDate, endDate) {
    try {
        const response = await sourceApi.get("/api/dataValueSets.json", {
            params: {
                dataSet: dataSetId,
                orgUnit: orgUnitId,
                startDate: startDate,
                endDate: endDate,
            },
        });
        return response.data.dataValues || [];
    } catch (error) {
        console.error(
            `Error fetching source data for orgUnit ${orgUnitId}, dataSet ${dataSetId}:`,
            error.message,
        );
        return [];
    }
}

// Function to post data to destination instance
async function postDataToDestination(dataValues) {
    try {
        await destApi.post(
            "/api/dataValueSets",
            { dataValues },
            { params: { async: true } },
        );
        console.log(
            `Successfully posted ${dataValues.length} data values to destination`,
        );
    } catch (error) {
        console.error("Error posting data to destination:", error.message);
    }
}

// Main function to copy data
async function copyData(dataSetIds, startDate, endDate) {
    const destOrgUnits = await getDestinationOrgUnits();
    for (const orgUnit of destOrgUnits) {
        for (const dataSetId of dataSetIds) {
            console.log(
                `Processing orgUnit: ${orgUnit.name}, dataSet: ${dataSetId}`,
            );
            const sourceData = await getSourceData(
                orgUnit.id,
                dataSetId,
                startDate,
                endDate,
            );
            if (sourceData.length > 0) {
                await postDataToDestination(sourceData);
            } else {
                console.log(
                    `No data found for orgUnit: ${orgUnit.name}, dataSet: ${dataSetId}`,
                );
            }
        }
    }
}

const dataSetIds = [
    "onFoQ4ko74y",
    "RtEYsASU7PG",
    "ic1BSWhGOso",
    "nGkMm2VBT4G",
    "VDhwrW9DiC1",
    "quMWqLxzcfO",
    "dFRD2A5fdvn",
    "DFMoIONIalm",
    "EBqVAQRmiPm",
];
const startDate = "2024-04-01";
const endDate = "2024-10-31";

copyData(dataSetIds, startDate, endDate)
    .then(() => {
        console.log("Data copy process completed");
    })
    .catch((error) => {
        console.error("Error in data copy process:", error.message);
    });
