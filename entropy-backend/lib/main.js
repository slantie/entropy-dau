import pkg from "@prisma/client";
const { PrismaClient } = pkg;
import { parseTransactionFile } from "./excelParser.js";
import { validateRow } from "./validator.js";
import fs from "fs";
import fsp from "fs/promises";
import csv from "csv-parser";

const prisma = new PrismaClient();

/**
 * Handle individual row ingestion and batch tracking
 */
async function processRow(row, context) {
  const { valid, data, error } = validateRow(row);
  if (!valid) {
    context.recordsFailed++;
    if (context.errors.length < 50) {
      context.errors.push({
        row: context.recordsFailed + context.recordsProcessed,
        error,
      });
    }
    return;
  }

  const { transaction, identity, vFeatures } = data;

  // Create transaction with nested relations
  const createData = { ...transaction };

  if (identity) {
    createData.identity = { create: identity };
  }

  if (vFeatures) {
    createData.vFeatures = { create: vFeatures };
  }

  try {
    await prisma.transaction.create({ data: createData });
    context.recordsProcessed++;

    // Periodically update dataset run progress (every 50 records)
    if (context.recordsProcessed % 50 === 0) {
      await prisma.datasetRun.update({
        where: { id: context.datasetRunId },
        data: { recordsProcessed: context.recordsProcessed },
      });
      console.log(`Progress: ${context.recordsProcessed} records...`);
    }
  } catch (insertError) {
    context.recordsFailed++;
    if (context.errors.length < 50) {
      context.errors.push({
        TransactionID: transaction.TransactionID?.toString(),
        error: insertError.message,
      });
    }
    console.error(
      `Insert error for Transaction ${transaction.TransactionID}:`,
      insertError.message
    );
  }
}

/**
 * Parse and ingest IEEE-CIS transaction data
 * Uses streaming for CSV to handle massive files without crashing memory
 * NO FEATURE ENGINEERING - raw data only for downstream processing
 */
export async function parseAndIngest(filePath, maxRows = 10000) {
  console.log(`\n=== IEEE-CIS Raw Data Ingestion Start ===`);
  let datasetRun = null;
  const context = {
    recordsProcessed: 0,
    recordsFailed: 0,
    errors: [],
    datasetRunId: null,
  };

  try {
    // Create dataset run tracker
    datasetRun = await prisma.datasetRun.create({
      data: {
        datasetName: `IEEE-CIS Upload - ${new Date().toISOString()}`,
        status: "RUNNING",
        recordsProcessed: 0,
      },
    });
    context.datasetRunId = datasetRun.id;
    console.log(`Dataset Run ID: ${datasetRun.id}`);

    // Check if CSV for streaming (Multer preserves extension now)
    const isCsv =
      filePath.toLowerCase().endsWith(".csv") ||
      filePath.toLowerCase().includes(".csv-");

    if (isCsv) {
      console.log("Streaming CSV ingestion...");
      await new Promise((resolve, reject) => {
        let rowsStreamed = 0;
        const stream = fs.createReadStream(filePath).pipe(csv());

        stream.on("data", async (row) => {
          if (rowsStreamed >= maxRows) {
            stream.destroy();
            resolve();
            return;
          }
          rowsStreamed++;

          // Pause stream while processing to avoid overwhelming DB/memory
          stream.pause();
          await processRow(row, context).catch((err) => {
            console.error("Error processing row:", err);
          });
          stream.resume();
        });

        stream.on("end", () => resolve());
        stream.on("error", (err) => reject(err));
      });
    } else {
      // For Excel files, use legacy loading (XLSX files are usually smaller)
      console.log("Loading Excel file...");
      const rows = await parseTransactionFile(filePath);
      const capped = rows.slice(0, maxRows);
      console.log(`Processing ${capped.length} rows...`);

      for (const row of capped) {
        await processRow(row, context);
      }
    }

    // Final update and mark as completed
    datasetRun = await prisma.datasetRun.update({
      where: { id: datasetRun.id },
      data: {
        status: "COMPLETED",
        recordsProcessed: context.recordsProcessed,
        endedAt: new Date(),
      },
    });

    console.log(`\n=== INGESTION COMPLETE ===`);
    console.log(`✓ Records processed: ${context.recordsProcessed}`);
    console.log(`✗ Records failed: ${context.recordsFailed}`);

    return {
      success: true,
      recordsProcessed: context.recordsProcessed,
      recordsFailed: context.recordsFailed,
      datasetRunId: datasetRun.id,
      errors: context.errors,
    };
  } catch (err) {
    console.error(`\n=== INGESTION FAILED ===`);
    console.error(`Error: ${err.message}`);

    if (datasetRun) {
      await prisma.datasetRun.update({
        where: { id: datasetRun.id },
        data: { status: "FAILED", endedAt: new Date() },
      });
    }
    throw err;
  } finally {
    try {
      if (fs.existsSync(filePath)) {
        await fsp.unlink(filePath);
        console.log(`Temp file cleaned up: ${filePath}`);
      }
    } catch (e) {
      console.warn(`Could not delete temp file: ${e.message}`);
    }
  }
}
