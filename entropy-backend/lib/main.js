import pkg from "@prisma/client";
const { PrismaClient } = pkg;
import { parseExcel } from "./excelParser.js";
import { validateRow } from "./validator.js";
import fs from "fs/promises";

const prisma = new PrismaClient();

export async function parseAndIngest(
  filePath,
  maxRows = 10000,
  batchSize = 100
) {
  console.log(`\nMain File Start`);
  let datasetRun = null;
  let recordsProcessed = 0;
  let recordsFailed = 0;
  const errors = [];

  try {
    datasetRun = await prisma.datasetRun.create({
      data: {
        datasetName: `Excel Upload - ${new Date().toISOString()}`,
        status: "RUNNING",
        recordsProcessed: 0,
      },
    });
    console.log(`Dataset Run ID: ${datasetRun.id}`);

    const rows = await parseExcel(filePath);
    const capped = rows.slice(0, maxRows);
    console.log(`Processing ${capped.length} rows (cap: ${maxRows})`);

    const validRows = [];
    for (const row of capped) {
      const { valid, data, error } = validateRow(row);
      if (valid) {
        validRows.push(data);
      } else {
        recordsFailed++;
        if (errors.length < 50) errors.push(error);
      }
    }

    console.log(`${validRows.length} valid, ${recordsFailed} failed`);

    const batches = [];
    for (let i = 0; i < validRows.length; i += batchSize) {
      batches.push(validRows.slice(i, i + batchSize));
    }

    console.log(`${batches.length} batches`);

    for (let idx = 0; idx < batches.length; idx++) {
      await prisma.transaction.createMany({ data: batches[idx] });
      recordsProcessed += batches[idx].length;

      await prisma.datasetRun.update({
        where: { id: datasetRun.id },
        data: { recordsProcessed },
      });

      console.log(
        `Batch ${idx + 1}/${batches.length} (total: ${recordsProcessed})`
      );
    }

    datasetRun = await prisma.datasetRun.update({
      where: { id: datasetRun.id },
      data: { status: "COMPLETED", endedAt: new Date() },
    });

    console.log(`\nDONE: ${recordsProcessed} transactions`);

    return {
      success: true,
      recordsProcessed,
      recordsFailed,
      datasetRunId: datasetRun.id,
      errors,
    };
  } catch (err) {
    console.error(`FAILED: ${err.message}`);
    if (datasetRun) {
      await prisma.datasetRun.update({
        where: { id: datasetRun.id },
        data: { status: "FAILED", endedAt: new Date() },
      });
    }
    throw err;
  } finally {
    try {
      await fs.unlink(filePath);
    } catch (e) {}
  }
}
