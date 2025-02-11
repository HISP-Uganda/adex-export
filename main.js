const axios = require("axios");
const dotenv = require("dotenv");
const { chunk, update } = require("lodash");
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

            const [level7Units, level6Units] = await Promise.all([
                this.fetchOrgUnits(7, "id,name"),
                this.fetchOrgUnits(6, "id,name,children"),
            ]);

            const childlessLevel6Units = level6Units.filter(
                (ou) => ou.children.length === 0,
            );
            const allUnits = [...childlessLevel6Units, ...level7Units];

            console.log(`Found ${allUnits.length} total organization units`);
            return allUnits;
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
            return data.response.importCount;
        } catch (error) {
            throw new Error(`Failed to upload data values: ${error.message}`);
        }
    }

    /**
     * Downloads and processes CSV data for an organization unit
     * @private
     */
    async downloadCSV(datasets, orgUnit, startDate, endDate, current, total) {
        const params = new URLSearchParams({
            orgUnit: orgUnit.id,
            startDate,
            endDate,
        });
        datasets.forEach((id) => params.append("dataSet", id));

        console.log(
            `Downloading data for ${orgUnit.name} (${current}/${total})...`,
        );

        try {
            const { data: csvData } = await this.sourceApi.get(
                `/api/dataValueSets.csv?${params.toString()}`,
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
                            // Process in batches
                            const batches = chunk(dataValues, this.batchSize);
                            let totalImported = 0;
                            let totalIgnored = 0;
                            let totalDeleted = 0;
                            let totalUpdated = 0;

                            for (const batch of batches) {
                                const result =
                                    await this.processDataValuesBatch(batch);
                                totalImported += result.imported || 0;
                                totalIgnored += result.ignored || 0;
                                totalDeleted += result.deleted || 0;
                                totalUpdated += result.updated || 0;
                            }

                            resolve({
                                imported: totalImported,
                                ignored: totalIgnored,
                                deleted: totalDeleted,
                                updated: totalUpdated,
                            });
                        } catch (error) {
                            reject(error);
                        }
                    },
                    error: reject,
                });
            });
        } catch (error) {
            throw new Error(
                `Failed to download/process data: ${error.message}`,
            );
        }
    }

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
            let totalImported = 0;
            let totalUpdated = 0;
            let totalIgnored = 0;
            let totalDeleted = 0;
            let errors = [];

            for (const [index, orgUnit] of orgUnits.entries()) {
                try {
                    const result = await this.downloadCSV(
                        datasets,
                        orgUnit,
                        startDate,
                        endDate,
                        index + 1,
                        orgUnits.length,
                    );
                    totalImported += result.imported;
                    totalIgnored += result.ignored;
                    totalDeleted += result.deleted;
                    totalUpdated += result.updated;
                    console.log(
                        `Imported ${result.imported} Ignored ${result.ignored} Deleted ${result.deleted} Updated ${result.updated} values for ${orgUnit.name}`,
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
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}

module.exports = DHIS2DataTransfer;
