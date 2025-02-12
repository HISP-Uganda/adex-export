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
            const { importCount, conflicts } = data.response;
            return { importCount, conflicts };
        } catch (error) {
            const { importCount, conflicts } = error.response.data.response;
            return { importCount, conflicts };
        }
    }

    /**
     * Downloads and processes CSV data for an organization unit
     * @private
     */
    async downloadCSV(dataSet, orgUnit, period, current, total) {
        try {
            const params = new URLSearchParams({
                orgUnit: orgUnit.id,
                period,
                dataSet,
                children: true,
            });
            console.log(
                `Downloading data for ${orgUnit.name} for dataset ${dataSet} (${current}/${total})...`,
            );
            const { data } = await this.sourceApi.get(
                `/api/dataValueSets.csv?${params.toString()}`,
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
                        const batches = chunk(dataValues, this.batchSize);
                        let totalImported = 0;
                        let totalIgnored = 0;
                        let totalDeleted = 0;
                        let totalUpdated = 0;
                        let conflicts = [];

                        for (const batch of batches) {
                            const result = await this.processDataValuesBatch(
                                batch,
                            );
                            totalImported += result.importCount.imported || 0;
                            totalIgnored += result.importCount.ignored || 0;
                            totalDeleted += result.importCount.deleted || 0;
                            totalUpdated += result.importCount.updated || 0;
                            conflicts = conflicts.concat(result.conflicts);
                        }
                        resolve({
                            imported: totalImported,
                            ignored: totalIgnored,
                            deleted: totalDeleted,
                            updated: totalUpdated,
                            conflicts,
                        });
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
    async transferData(dataSet, period) {
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
                        dataSet,
                        orgUnit,
                        period,
                        index + 1,
                        orgUnits.length,
                    );
                    const { imported, ignored, deleted, updated, conflicts } =
                        result;
                    totalImported += imported;
                    totalIgnored += ignored;
                    totalDeleted += deleted;
                    totalUpdated += updated;
                    console.log(conflicts);
                    console.log(
                        `Imported ${imported} Ignored ${ignored} Deleted ${deleted} Updated ${updated} values for ${orgUnit.name}`,
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

async function main(dataSet, periods) {
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
        for (const period of periods) {
            const result = await transfer.transferData(dataSet, period);
            console.log(result);
        }
    } catch (error) {
        console.error("Transfer failed:", error.message);
        process.exit(1);
    }
}

if (require.main === module) {
    const args = process.argv.slice(2);
    if (args[1] === "monthly") {
        main(args[0], process.env.MONTHLY_PERIODS.split(","));
    } else if (args[1] === "quarterly") {
        main(args[0], process.env.QUARTERLY_PERIODS.split(","));
    }
}

module.exports = DHIS2DataTransfer;
