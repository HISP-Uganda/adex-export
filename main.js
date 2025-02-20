const axios = require("axios");
const dotenv = require("dotenv");
const { chunk, orderBy } = require("lodash");
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
            console.log(
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
                ds.dataSetElements.map((de) => de.dataElement.id),
            );
        } catch (error) {
            console.log(
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
        }
    }

    /**
     * Processes a batch of data values
     * @private
     */
    async processDataValuesBatch(dataValues) {
        console.log(`Importing ${dataValues.length} data values...`);
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
            if (
                error &&
                error.response &&
                error.response.data &&
                error.response.data.response
            ) {
                if (error.response.data.response.conflicts) {
                    console.log(error.response.data.response.conflicts);
                }

                if (error.response.data.response.importCount) {
                    console.log(error.response.data.response.importCount);
                }
            }
        }
    }

    /**
     * Downloads and processes CSV data for an organization unit
     * @private
     */
    async downloadCSV(
        datasets,
        orgUnit,
        startDate,
        endDate,
        current,
        total,
        dataElements,
    ) {
        const params = new URLSearchParams({
            orgUnit: orgUnit.id,
            startDate,
            endDate,
            children: true,
        });
        datasets.forEach((id) => params.append("dataSet", id));

        console.log(
            `Downloading data for ${orgUnit.name} (${current}/${total})...`,
        );

        try {
            const { data: csvData } = await this.sourceApi.get(
                `/api/dataValueSets.csv?${params.toString()}`,
            );
            console.log(
                `Finished Downloading data for ${orgUnit.name} (${current}/${total})...`,
            );

            return new Promise((resolve, reject) => {
                const dataValues = [];

                Papa.parse(csvData, {
                    header: true,
                    skipEmptyLines: true,
                    transform: (value) => value.trim(),
                    step: ({ data }) => {
                        if (this.isValidDataValue(data)) {
                            dataValues.push(this.normalizeDataValue(data));
                        }
                    },
                    complete: async () => {
                        try {
                            const batches = chunk(
                                dataValues.filter((dv) =>
                                    dataElements.includes(dv.dataElement),
                                ),
                                this.batchSize,
                            );

                            for (const batch of batches) {
                                await this.processDataValuesBatch(batch);
                            }

                            resolve({
                                processed: true,
                            });
                        } catch (error) {
                            reject(error);
                        }
                    },
                    error: reject,
                });
            });
        } catch (error) {
            console.log(`Failed to download/process data: ${error.message}`);
        }
    }

    /**
     * Validates data value row
     * @private
     */
    /**
     * Validates data value row
     * @private
     */
    isValidDataValue(data) {
        return [
            "dataelement",
            "period",
            "orgunit",
            "value",
            "categoryoptioncombo",
            "attributeoptioncombo",
        ].every((field) => Boolean(data[field]?.trim()));
    }

    /**
     * Normalizes data value object
     * @private
     */
    normalizeDataValue(data) {
        return {
            dataElement: data.dataelement,
            period: data.period,
            orgUnit: data.orgunit,
            categoryOptionCombo: data.categoryoptioncombo,
            attributeOptionCombo: data.attributeoptioncombo,
            value: data.value.trim(),
            storedBy: data.storedby,
            lastUpdated: data.lastupdated,
            comment: data.comment,
            followup: data.followup,
        };
    }

    /**
     * Transfers data between DHIS2 instances
     */
    async transferData(datasets, startDate, endDate) {
        try {
            const orgUnits = await this.getOrganisations();
            const dataElements = await this.fetchDataElements();
            let totalImported = 0;
            let totalUpdated = 0;
            let totalIgnored = 0;
            let totalDeleted = 0;
            let errors = [];

            for (const [index, orgUnit] of orgUnits.entries()) {
                try {
                    await this.downloadCSV(
                        datasets,
                        orgUnit,
                        startDate,
                        endDate,
                        index + 1,
                        orgUnits.length,
                        dataElements,
                    );
                } catch (error) {
                    errors.push({
                        orgUnit: orgUnit.name,
                        error: error.message,
                    });
                    console.error(
                        `Error processing ${orgUnit.name}:`,
                        error.message,
                    );
                }
            }

            return {
                totalImported,
                totalUpdated,
                totalIgnored,
                totalDeleted,
                totalOrgUnits: orgUnits.length,
                errors: errors.length ? errors : undefined,
            };
        } catch (error) {
            console.log(`Transfer failed: ${error.message}`);
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

        const datasets = [
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

        const transfer = new DHIS2DataTransfer(configs.source, configs.dest);
        const result = await transfer.transferData(
            datasets,
            "2024-01-01",
            "2024-12-31",
        );
        console.log("Transfer completed:", result);
    } catch (error) {
        console.error("Transfer failed:", error.message);
    }
}

if (require.main === module) {
    main();
}

module.exports = DHIS2DataTransfer;
