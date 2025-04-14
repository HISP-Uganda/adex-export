const axios = require("axios");
const dotenv = require("dotenv");
const { chunk } = require("lodash");
const Papa = require("papaparse");

dotenv.config();

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
                    params: { async: true },
                },
            );
            console.log(JSON.stringify(data));
        } catch (error) {
            console.log(error.response.data);
            console.log(error.message);
        }
    }

    /**
     * Downloads and processes CSV data for an organization unit
     * @private
     */
    async downloadCSV(datasets, orgUnit, current, total) {
        const params = new URLSearchParams({
            orgUnit,
            lastUpdatedDuration: "1d",
            children: "true",
        });
        datasets.forEach((id) => params.append("dataSet", id));

        console.log(
            `Downloading data for ${orgUnit.name} (${current}/${total})...`,
        );

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
                        const batches = chunk(dataValues, this.batchSize);
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
    async transferData(datasets) {
        try {
            const organisationUnits = await this.getOrganisations();
            let errors = [];
            for (const [index, orgUnit] of organisationUnits.entries()) {
                try {
                    await this.downloadCSV(
                        datasets,
                        orgUnit.id,
                        index + 1,
                        organisationUnits.length,
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
            return errors;
        } catch (error) {
            console.log(`Transfer failed: ${error.message}`);
        }
    }
}

async function main() {
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
        datasets
    );
    console.log("Transfer completed:", result);
}

if (require.main === module) {
    main();
}

module.exports = DHIS2DataTransfer;
