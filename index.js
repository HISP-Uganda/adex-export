const axios = require("axios");
const dotenv = require("dotenv");
const csv = require("csv-parser");

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
async function getOrganisationUnits() {
    try {
        const response = await sourceApi.get(
            "/api/organisationUnits.json?fields=id,name&paging=false&filter=level:in:[5,6]",
        );
        return response.data.organisationUnits;
    } catch (error) {
        console.error("Error fetching destination org units:", error.message);
        return [];
    }
}

// Function to get data for a specific org unit and dataset from source instance
async function getSourceData(orgUnitId, dataSets, startDate, endDate) {
    const dataSetParams = dataSets.map((ds) => `dataSet=${ds}`).join("&");
    const url = `/api/dataValueSets.csv?${dataSetParams}&orgUnit=${orgUnitId}&startDate=${startDate}&endDate=${endDate}`;
    try {
        const { data } = await sourceApi.get(url);
        return data;
    } catch (error) {
        console.error(
            `Error fetching source data for orgUnit ${orgUnitId}, dataSets ${dataSets
                .map((ds) => ds)
                .join(",")}`,
            error.message,
        );
        return [];
    }
}

// Function to post data to destination instance
async function postDataToDestination(dataValues) {
    try {
        await destApi.post("/api/dataValueSets", dataValues, {
            params: { async: true },
            headers: { "Content-Type": "application/csv" },
        });
        console.log(`Successfully posted  data values to destination`);
    } catch (error) {
        console.error(
            "Error posting data to destination:",
            error.response.data.response.conflicts,
        );
    }
}

async function copyData(dataSetIds, startDate, endDate) {
    const destinationOrgUnits = await getOrganisationUnits();
    for (const { id, name } of destinationOrgUnits) {
        console.log(name);
        const sourceData = await getSourceData(
            id,
            dataSetIds,
            startDate,
            endDate,
        );
        await postDataToDestination(sourceData);
    }
}

const dataSetIds = [
    "onFoQ4ko74y",
    "RtEYsASU7PG",
    "ic1BSWhGOso",
    "nGkMm2VBT4G",
    "quMWqLxzcfO",
    "dFRD2A5fdvn",
    "DFMoIONIalm",
    "EBqVAQRmiPm",
];
const startDate = "2024-01-01";
const endDate = "2024-10-31";

copyData(dataSetIds, startDate, endDate)
    .then(() => {
        console.log("Data copy process completed");
    })
    .catch((error) => {
        console.error("Error in data copy process:", error.message);
    });
