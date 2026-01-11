import XLSX from "xlsx";
import csv from "csv-parser";
import fs from "fs";

/**
 * Parse IEEE-CIS Excel/CSV file containing raw transaction features
 * Supports both train_transaction.csv format and combined formats
 * Uses streaming for CSV files to avoid memory issues with large files
 */
export async function parseTransactionFile(path) {
  console.log("Parsing transaction file at:", path);

  // If it's a CSV, use streaming parser
  const isCsv =
    path.toLowerCase().endsWith(".csv") || path.toLowerCase().includes(".csv-");

  if (isCsv) {
    return new Promise((resolve, reject) => {
      const results = [];
      fs.createReadStream(path)
        .pipe(csv())
        .on("data", (data) => results.push(data))
        .on("end", () => {
          console.log(`${results.length} transactions processed from CSV.`);
          resolve(results);
        })
        .on("error", (err) => reject(err));
    });
  }

  // For Excel files, use XLSX (usually smaller than raw CSVs)
  const workbook = XLSX.readFile(path);
  const worksheet = workbook.Sheets[workbook.SheetNames[0]];
  const transactions = XLSX.utils.sheet_to_json(worksheet);
  console.log(`${transactions.length} transactions processed from Excel.`);
  return transactions;
}

/**
 * XGB96 Raw Feature Column Mapping
 * Maps CSV/Excel columns to database schema fields
 * STORES ONLY RAW FEATURES - NO ENGINEERED FEATURES
 */

// Core transaction fields
export const CORE_COLUMNS = {
  TransactionID: "TransactionID",
  TransactionDT: "TransactionDT",
  TransactionAmt: "TransactionAmt",
  ProductCD: "ProductCD",
};

// Card information
export const CARD_COLUMNS = {
  card1: "card1",
  card2: "card2",
  card3: "card3",
  card4: "card4",
  card5: "card5",
  card6: "card6",
};

// Address and distance
export const ADDR_COLUMNS = {
  addr1: "addr1",
  addr2: "addr2",
  dist1: "dist1",
  dist2: "dist2",
};

// Email domains
export const EMAIL_COLUMNS = {
  P_emaildomain: "P_emaildomain",
  R_emaildomain: "R_emaildomain",
};

// Count features (C1-C14)
export const C_COLUMNS = {
  C1: "C1",
  C2: "C2",
  C3: "C3",
  C4: "C4",
  C5: "C5",
  C6: "C6",
  C7: "C7",
  C8: "C8",
  C9: "C9",
  C10: "C10",
  C11: "C11",
  C12: "C12",
  C13: "C13",
  C14: "C14",
};

// Time delta features (D1-D15)
export const D_COLUMNS = {
  D1: "D1",
  D2: "D2",
  D3: "D3",
  D4: "D4",
  D5: "D5",
  D6: "D6",
  D7: "D7",
  D8: "D8",
  D9: "D9",
  D10: "D10",
  D11: "D11",
  D12: "D12",
  D13: "D13",
  D14: "D14",
  D15: "D15",
};

// Match flags (M1-M9)
export const M_COLUMNS = {
  M1: "M1",
  M2: "M2",
  M3: "M3",
  M4: "M4",
  M5: "M5",
  M6: "M6",
  M7: "M7",
  M8: "M8",
  M9: "M9",
};

// Identity features (id_01 - id_38) - from identity table
export const ID_COLUMNS = {
  id_01: "id_01",
  id_02: "id_02",
  id_03: "id_03",
  id_04: "id_04",
  id_05: "id_05",
  id_06: "id_06",
  id_07: "id_07",
  id_08: "id_08",
  id_09: "id_09",
  id_10: "id_10",
  id_11: "id_11",
  id_12: "id_12",
  id_13: "id_13",
  id_14: "id_14",
  id_15: "id_15",
  id_16: "id_16",
  id_17: "id_17",
  id_18: "id_18",
  id_19: "id_19",
  id_20: "id_20",
  id_21: "id_21",
  id_22: "id_22",
  id_23: "id_23",
  id_24: "id_24",
  id_25: "id_25",
  id_26: "id_26",
  id_27: "id_27",
  id_28: "id_28",
  id_29: "id_29",
  id_30: "id_30",
  id_31: "id_31",
  id_32: "id_32",
  id_33: "id_33",
  id_34: "id_34",
  id_35: "id_35",
  id_36: "id_36",
  id_37: "id_37",
  id_38: "id_38",
};

// Device information
export const DEVICE_COLUMNS = {
  DeviceType: "DeviceType",
  DeviceInfo: "DeviceInfo",
};

// Selected V columns required by XGB96 model
export const V_COLUMNS = {
  V1: "V1",
  V3: "V3",
  V4: "V4",
  V6: "V6",
  V8: "V8",
  V11: "V11",
  V13: "V13",
  V14: "V14",
  V17: "V17",
  V20: "V20",
  V23: "V23",
  V26: "V26",
  V27: "V27",
  V30: "V30",
  V36: "V36",
  V37: "V37",
  V40: "V40",
  V41: "V41",
  V44: "V44",
  V47: "V47",
  V48: "V48",
  V54: "V54",
  V56: "V56",
  V59: "V59",
  V62: "V62",
  V65: "V65",
  V67: "V67",
  V68: "V68",
  V70: "V70",
  V76: "V76",
  V78: "V78",
  V80: "V80",
  V82: "V82",
  V86: "V86",
  V88: "V88",
  V89: "V89",
  V91: "V91",
  V107: "V107",
  V108: "V108",
  V111: "V111",
  V115: "V115",
  V117: "V117",
  V120: "V120",
  V121: "V121",
  V123: "V123",
  V124: "V124",
  V127: "V127",
  V129: "V129",
  V130: "V130",
  V136: "V136",
  V138: "V138",
  V139: "V139",
  V142: "V142",
  V147: "V147",
  V156: "V156",
  V160: "V160",
  V162: "V162",
  V165: "V165",
  V166: "V166",
  V169: "V169",
  V171: "V171",
  V173: "V173",
  V175: "V175",
  V176: "V176",
  V178: "V178",
  V180: "V180",
  V182: "V182",
  V185: "V185",
  V187: "V187",
  V188: "V188",
  V198: "V198",
  V203: "V203",
  V205: "V205",
  V207: "V207",
  V209: "V209",
  V210: "V210",
  V215: "V215",
  V218: "V218",
  V220: "V220",
  V221: "V221",
  V223: "V223",
  V224: "V224",
  V226: "V226",
  V228: "V228",
  V229: "V229",
  V234: "V234",
  V235: "V235",
  V238: "V238",
  V240: "V240",
  V250: "V250",
  V252: "V252",
  V253: "V253",
  V257: "V257",
  V258: "V258",
  V260: "V260",
  V261: "V261",
  V264: "V264",
  V266: "V266",
  V267: "V267",
  V271: "V271",
  V274: "V274",
  V277: "V277",
  V281: "V281",
  V283: "V283",
  V284: "V284",
  V285: "V285",
  V286: "V286",
  V289: "V289",
  V291: "V291",
  V294: "V294",
  V296: "V296",
  V297: "V297",
  V301: "V301",
  V303: "V303",
  V305: "V305",
  V307: "V307",
  V309: "V309",
  V310: "V310",
  V314: "V314",
  V320: "V320",
};

// Ground truth label
export const LABEL_COLUMN = {
  isFraud: "isFraud",
};

// Combined mapping for all columns
export const ALL_COLUMNS = {
  ...CORE_COLUMNS,
  ...CARD_COLUMNS,
  ...ADDR_COLUMNS,
  ...EMAIL_COLUMNS,
  ...C_COLUMNS,
  ...D_COLUMNS,
  ...M_COLUMNS,
  ...ID_COLUMNS,
  ...DEVICE_COLUMNS,
  ...V_COLUMNS,
  ...LABEL_COLUMN,
};
