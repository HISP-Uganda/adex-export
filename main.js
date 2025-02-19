const axios = require("axios");
const dotenv = require("dotenv");
const { chunk, update, result } = require("lodash");
const Papa = require("papaparse");

dotenv.config();

const dataSetPeriods = new Map();
dataSetPeriods.set("onFoQ4ko74y", "quarterly");
dataSetPeriods.set("RtEYsASU7PG", "monthly");
dataSetPeriods.set("ic1BSWhGOso", "monthly");
dataSetPeriods.set("nGkMm2VBT4G", "monthly");
dataSetPeriods.set("VDhwrW9DiC1", "monthly");
dataSetPeriods.set("quMWqLxzcfO", "monthly");
dataSetPeriods.set("dFRD2A5fdvn", "quarterly");
dataSetPeriods.set("DFMoIONIalm", "quarterly");
dataSetPeriods.set("EBqVAQRmiPm", "monthly");

class DHIS2DataTransfer {
    static DEFAULT_BATCH_SIZE = 1000;

    /**
     * @typedef {Object} DHISConfig
     * @property {string} url
     * @property {string} username
     * @property {string} password
     */

    /**
     * @param {DHISConfig} sourceConfig
     * @param {DHISConfig} destConfig
     * @param {number} [batchSize=1000]
     */
    constructor(
        sourceConfig,
        destConfig,
        batchSize = DHIS2DataTransfer.DEFAULT_BATCH_SIZE,
    ) {
        this.batchSize = batchSize;
        this.sourceApi = this.createAxiosInstance(sourceConfig, true);
        this.destApi = this.createAxiosInstance(destConfig);
    }

    /**
     * Creates an axios instance with DHIS2 configuration
     * @private
     */
    createAxiosInstance(config, isSource = false) {
        return axios.create({
            baseURL: config.url.replace(/\/$/, ""),
            auth: {
                username: config.username,
                password: config.password,
            },
            responseType: isSource ? "text" : "json",
            headers: {
                Accept: isSource ? "text/csv" : "application/json",
                "X-Requested-With": "XMLHttpRequest",
            },
        });
    }

    /**
     * Fetches organisation units from both levels
     * @private
     */
    async fetchOrgUnits(level, fields) {
        const url = `/api/organisationUnits.json`;
        const params = {
            fields,
            paging: false,
            level,
        };

        try {
            const { data } = await this.destApi.get(url, { params });
            return data.organisationUnits;
        } catch (error) {
            throw new Error(
                `Failed to fetch level ${level} organization units: ${error.message}`,
            );
        }
    }
    async fetchDataElements() {
        console.log("Fetching data elements...");
        const url = `/api/dataSets.json`;
        const params = {
            fields: "id,dataSetElements[dataElement[id,name]]",
            paging: false,
            filter: `id:in:[onFoQ4ko74y,RtEYsASU7PG,ic1BSWhGOso,nGkMm2VBT4G,VDhwrW9DiC1,quMWqLxzcfO,dFRD2A5fdvn,DFMoIONIalm,EBqVAQRmiPm]`,
        };

        try {
            const { data } = await this.destApi.get(url, { params });
            return data.dataSets.flatMap((ds) =>
                ds.dataSetElements.map((de) => {
                    return {
                        id: de.dataElement.id,
                        name: de.dataElement.name,
                        period: dataSetPeriods.get(ds.id),
                    };
                }),
            );
        } catch (error) {
            throw new Error(
                `Failed to fetch level ${level} organization units: ${error.message}`,
            );
        }
    }

    /**
     * Gets combined organisation units
     */
    async getOrganisations() {
        try {
            console.log("Fetching organisation units...");
            const units = await this.fetchOrgUnits(3, "id,name");
            return units;
        } catch (error) {
            console.error("Failed to fetch organization units:", error.message);
            throw error;
        }
    }

    /**
     * Processes a batch of data values
     * @private
     */
    async processDataValuesBatch(dataValues) {
        if (!dataValues.length) return { imported: 0 };

        try {
            const { data } = await this.destApi.post(
                "/api/dataValueSets",
                { dataValues },
                {
                    headers: { "Content-Type": "application/json" },
                    params: { async: false },
                },
            );
            const { importCount } = data.response;
            console.log(importCount);
        } catch (error) {
            console.log(error.message);
        }
    }

    /**
     * Downloads and processes CSV data for an organization unit
     * @private
     */
    async downloadCSV({ id, name, total, current }) {
        try {
            console.log(`Downloading data for ${name} (${current}/${total})`);
            const { data } = await this.sourceApi.get(
                `/api/sqlViews/wiwtLN4FKl5/data.csv`,
                { params: { var: `dx:${id}` } },
            );
            console.log(
                `Finished Downloading data for ${name} (${current}/${total})`,
            );

            return new Promise((resolve, reject) => {
                const dataValues = [];

                Papa.parse(data, {
                    header: true,
                    skipEmptyLines: true,
                    transform: (value) => value.trim(),
                    step: ({ data }) => {
                        if (this.isValidDataValue(data)) {
                            dataValues.push(this.normalizeDataValue(data));
                        }
                    },
                    complete: async () => {
                        const results = [];
                        const batches = chunk(dataValues, this.batchSize);

                        for (const batch of batches) {
                            const result = await this.processDataValuesBatch(
                                batch,
                            );
                            results.push(result);
                        }
                        resolve(results);
                    },
                    error: reject,
                });
            });
        } catch (error) {
            console(error.message);
        }
    }

    /**
     * Validates data value row
     * @private
     */
    isValidDataValue(data) {
        return ["dx", "pe", "ou", "value", "co", "ao"].every((field) =>
            Boolean(data[field]?.trim()),
        );
    }

    /**
     * Normalizes data value object
     * @private
     */
    normalizeDataValue(data) {
        return {
            dataElement: data.dx,
            period: data.pe,
            orgUnit: data.ou,
            categoryOptionCombo: data.co,
            attributeOptionCombo: data.ao,
            value: data.value.trim().split(".")[0],
        };
    }

    /**
     * Transfers data between DHIS2 instances
     */
    async transferData() {
        try {
            const dataElements = await this.fetchDataElements();
            let allResults = [];
            for (const [index, { id, name }] of dataElements
                .reverse()
                .entries()) {
                const results = await this.downloadCSV({
                    id,
                    name,
                    total: dataElements.length,
                    current: index + 1,
                });
                allResults = allResults.concat(results);
            }
            return allResults;
        } catch (error) {
            throw new Error(`Transfer failed: ${error.message}`);
        }
    }
}

async function main() {
    try {
        const configs = {
            source: {
                url: process.env.SOURCE_DHIS2_URL,
                username: process.env.SOURCE_DHIS2_USERNAME,
                password: process.env.SOURCE_DHIS2_PASSWORD,
            },
            dest: {
                url: process.env.DEST_DHIS2_URL,
                username: process.env.DEST_DHIS2_USERNAME,
                password: process.env.DEST_DHIS2_PASSWORD,
            },
        };
        const transfer = new DHIS2DataTransfer(configs.source, configs.dest);
        const result = await transfer.transferData();
        console.log(result);
    } catch (error) {
        console.error("Transfer failed:", error.message);
    }
}

if (require.main === module) {
    main();
}

module.exports = DHIS2DataTransfer;
