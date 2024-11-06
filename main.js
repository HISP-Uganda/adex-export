const axios = require("axios");
const fs = require("fs").promises;
const csv = require("csv-parse");
const { createReadStream } = require("fs");
const dotenv = require("dotenv");
const { chunk } = require("lodash");
dotenv.config();

class DHIS2DataTransfer {
    constructor(sourceConfig, destConfig, batchSize = 1000) {
        this.sourceApi = axios.create({
            baseURL: sourceConfig.url.replace(/\/$/, ""),
            auth: {
                username: sourceConfig.username,
                password: sourceConfig.password,
            },
            responseType: "arraybuffer",
        });

        this.destApi = axios.create({
            baseURL: destConfig.url.replace(/\/$/, ""),
            auth: {
                username: destConfig.username,
                password: destConfig.password,
            },
        });

        this.batchSize = batchSize;
    }

    async downloadCSV(dataset, orgUnit, startDate, endDate) {
        try {
            console.log(
                `Downloading CSV data for dataset ${dataset} and orgUnit ${orgUnit.name}...`,
            );

            const url = `/api/dataValueSets.csv?${dataset
                .map((id) => `dataSet=${id}`)
                .join("&")}&orgUnit=${
                orgUnit.id
            }&startDate=${startDate}&endDate=${endDate}`;
            const response = await this.sourceApi.get(url);

            const filename = `dhis2_data_${Date.now()}.csv`;
            await fs.writeFile(filename, response.data);
            console.log(`CSV downloaded successfully to ${filename}`);
            return filename;
        } catch (error) {
            console.error("Error downloading CSV:", error.message);
            throw error;
        }
    }

    async getOrganisations() {
        try {
            console.log("Downloading organisation data...");
            const {
                data: { organisationUnits: ous1 },
            } = await this.destApi.get(
                `/api/organisationUnits.json?fields=id,name&paging=false&level=5`,
            );
            const {
                data: { organisationUnits: ous2 },
            } = await this.destApi.get(
                `/api/organisationUnits.json?fields=id,name&paging=false&level=6`,
            );

            return [...ous1, ...ous2];
        } catch (error) {
            console.error("Error downloading CSV:", error.message);
            throw error;
        }
    }

    async processCSVStream(filePath) {
        return new Promise((resolve, reject) => {
            let currentBatch = [];
            let totalProcessed = 0;
            let batchNumber = 1;
            let headers = null;

            // Create read stream
            const fileStream = fs.createReadStream(filePath);

            // Create parser
            const parser = csv.parse({
                delimiter: ",",
                trim: true,
                skip_empty_lines: true,
            });

            // Handle stream events
            const processRow = async (row) => {
                try {
                    if (!headers) {
                        headers = row;
                        console.log("Headers:", headers);
                        return;
                    }

                    // Convert row to object using headers
                    const dataValue = {};
                    headers.forEach((header, index) => {
                        if (row[index]) {
                            dataValue[header] = row[index];
                        }
                    });

                    // Validate required fields
                    if (this.validateDataValue(dataValue)) {
                        currentBatch.push(dataValue);
                        totalProcessed++;

                        // Process batch if it reaches batch size
                        if (currentBatch.length >= this.batchSize) {
                            parser.pause(); // Pause parsing while uploading
                            await this.processBatch(currentBatch, batchNumber);
                            currentBatch = [];
                            batchNumber++;
                            parser.resume(); // Resume parsing
                        }
                    }
                } catch (error) {
                    parser.destroy(error);
                }
            };

            // Stream pipeline
            fileStream
                .pipe(parser)
                .on("data", async (row) => {
                    try {
                        await processRow(row);
                    } catch (error) {
                        parser.destroy(error);
                    }
                })
                .on("end", async () => {
                    try {
                        // Process any remaining data in the last batch
                        if (currentBatch.length > 0) {
                            await this.processBatch(currentBatch, batchNumber);
                        }
                        console.log(
                            `Completed processing ${totalProcessed} records in ${batchNumber} batches`,
                        );
                        resolve({
                            status: "success",
                            totalProcessed,
                            batches: batchNumber,
                        });
                    } catch (error) {
                        reject(error);
                    }
                })
                .on("error", (error) => {
                    console.error("Error processing CSV:", error);
                    reject(error);
                });
        });
    }

    async uploadJSONBatch(batch, batchNumber, totalBatches) {
        try {
            console.log(
                `Uploading batch ${batchNumber}/${totalBatches} (${batch.length} records)...`,
            );

            const response = await this.destApi.post(
                "/api/dataValueSets",
                { dataValues: batch },
                {
                    headers: {
                        "Content-Type": "application/json",
                    },
                },
            );

            console.log(
                `Batch ${batchNumber} upload completed:`,
                response.data,
            );
            return response.data;
        } catch (error) {
            console.error(
                `Error uploading batch ${batchNumber}:`,
                error.message,
            );
            if (error.response) {
                console.error("Response:", error.response.data);
            }
            throw error;
        }
    }

    async transferData(dataset, startDate, endDate) {
        let csvFile = null;
        // try {
        const orgs = await this.getOrganisations();

        for (const orgUnit of orgs) {
            // Step 1: Download CSV
            csvFile = await this.downloadCSV(
                dataset,
                orgUnit,
                startDate,
                endDate,
            );

            // Step 2: Stream and convert CSV to JSON
            console.log("Converting CSV to JSON...");
            const dataValues = await this.streamCSVToJSON(csvFile);

            // console.log(dataValues);

            // if (dataValues.length === 0) {
            //     console.log("No valid data values found in CSV");
            //     // return {
            //     //     status: "completed",
            //     //     message: "No data to transfer",
            //     // };
            // }

            // Step 3: Upload in batches
            // const batches = chunk(dataValues, this.batchSize);
            // for (let i = 0; i < dataValues.length; i += this.batchSize) {
            //     batches.push(dataValues.slice(i, i + this.batchSize));
            // }

            // console.log(`Preparing to upload ${batches.length} batches...`);

            // const results = [];
            // for (let i = 0; i < batches.length; i++) {
            //     const result = await this.uploadJSONBatch(
            //         batches[i],
            //         i + 1,
            //         batches.length,
            //     );
            //     results.push(result);
            // }

            // // Step 4: Cleanup
            // await fs.unlink(csvFile);

            // console.log({
            //     status: "completed",
            //     totalRecords: dataValues.length,
            //     totalBatches: batches.length,
            //     results: results,
            // });
        }
        // } catch (error) {
        //     console.error("Transfer failed:", error.message);
        //     if (csvFile) {
        //         try {
        //             await fs.unlink(csvFile);
        //         } catch (cleanupError) {
        //             console.error(
        //                 "Error cleaning up file:",
        //                 cleanupError.message,
        //             );
        //         }
        //     }
        //     throw error;
        // }
    }
}

// Example usage
async function main() {
    try {
        const sourceConfig = {
            url: process.env.SOURCE_DHIS2_URL,
            username: process.env.SOURCE_DHIS2_USERNAME,
            password: process.env.SOURCE_DHIS2_PASSWORD,
        };

        const destConfig = {
            url: process.env.DEST_DHIS2_URL,
            username: process.env.DEST_DHIS2_USERNAME,
            password: process.env.DEST_DHIS2_PASSWORD,
        };
        const transfer = new DHIS2DataTransfer(sourceConfig, destConfig, 1000);
        const result = await transfer.transferData(
            [
                "onFoQ4ko74y",
                "RtEYsASU7PG",
                "ic1BSWhGOso",
                "nGkMm2VBT4G",
                "VDhwrW9DiC1",
                "quMWqLxzcfO",
                "dFRD2A5fdvn",
                "DFMoIONIalm",
                "EBqVAQRmiPm",
            ],
            "2024-01-01",
            "2024-10-31",
        );
        console.log("Transfer completed:", result);
    } catch (error) {
        console.error("Main process error:", error.message);
        process.exit(1);
    }
}

// Run the script if called directly
if (require.main === module) {
    main();
}

module.exports = DHIS2DataTransfer;
