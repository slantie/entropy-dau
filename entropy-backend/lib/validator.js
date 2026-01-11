import { v4 as uuidv4 } from "uuid";
import {
  CORE_COLUMNS,
  CARD_COLUMNS,
  ADDR_COLUMNS,
  EMAIL_COLUMNS,
  C_COLUMNS,
  D_COLUMNS,
  M_COLUMNS,
  ID_COLUMNS,
  DEVICE_COLUMNS,
  V_COLUMNS,
  LABEL_COLUMN,
} from "./excelParser.js";

/**
 * Validate and normalize a raw transaction row from IEEE-CIS dataset
 * Handles null/undefined/missing values properly
 * Returns structured data for Transaction, TransactionIdentity, and TransactionVFeatures tables
 */
export function validateRow(rawRow) {
  try {
    // Validate core required fields
    if (!rawRow.TransactionID) {
      return { valid: false, error: "Missing TransactionID" };
    }
    if (rawRow.TransactionDT === undefined || rawRow.TransactionDT === null) {
      return { valid: false, error: "Missing TransactionDT" };
    }
    if (
      rawRow.TransactionAmt === undefined ||
      rawRow.TransactionAmt === null ||
      rawRow.TransactionAmt < 0
    ) {
      return { valid: false, error: "Invalid TransactionAmt" };
    }

    // Build normalized transaction data
    const transaction = {
      id: uuidv4(),

      // Core fields
      TransactionID: toBigInt(rawRow.TransactionID),
      TransactionDT: toBigInt(rawRow.TransactionDT),
      TransactionAmt: toDecimal(rawRow.TransactionAmt),
      ProductCD: toString(rawRow.ProductCD),

      // Card information
      card1: toInt(rawRow.card1),
      card2: toInt(rawRow.card2),
      card3: toInt(rawRow.card3),
      card4: toString(rawRow.card4),
      card5: toInt(rawRow.card5),
      card6: toString(rawRow.card6),

      // Address & Distance
      addr1: toInt(rawRow.addr1),
      addr2: toInt(rawRow.addr2),
      dist1: toFloat(rawRow.dist1),
      dist2: toFloat(rawRow.dist2),

      // Email domains
      P_emaildomain: toString(rawRow.P_emaildomain),
      R_emaildomain: toString(rawRow.R_emaildomain),

      // C columns
      C1: toFloat(rawRow.C1),
      C2: toFloat(rawRow.C2),
      C3: toFloat(rawRow.C3),
      C4: toFloat(rawRow.C4),
      C5: toFloat(rawRow.C5),
      C6: toFloat(rawRow.C6),
      C7: toFloat(rawRow.C7),
      C8: toFloat(rawRow.C8),
      C9: toFloat(rawRow.C9),
      C10: toFloat(rawRow.C10),
      C11: toFloat(rawRow.C11),
      C12: toFloat(rawRow.C12),
      C13: toFloat(rawRow.C13),
      C14: toFloat(rawRow.C14),

      // D columns
      D1: toFloat(rawRow.D1),
      D2: toFloat(rawRow.D2),
      D3: toFloat(rawRow.D3),
      D4: toFloat(rawRow.D4),
      D5: toFloat(rawRow.D5),
      D6: toFloat(rawRow.D6),
      D7: toFloat(rawRow.D7),
      D8: toFloat(rawRow.D8),
      D9: toFloat(rawRow.D9),
      D10: toFloat(rawRow.D10),
      D11: toFloat(rawRow.D11),
      D12: toFloat(rawRow.D12),
      D13: toFloat(rawRow.D13),
      D14: toFloat(rawRow.D14),
      D15: toFloat(rawRow.D15),

      // M columns
      M1: toChar(rawRow.M1),
      M2: toChar(rawRow.M2),
      M3: toChar(rawRow.M3),
      M4: toChar(rawRow.M4),
      M5: toChar(rawRow.M5),
      M6: toChar(rawRow.M6),
      M7: toChar(rawRow.M7),
      M8: toChar(rawRow.M8),
      M9: toChar(rawRow.M9),

      // Ground truth label
      isFraud: toBoolean(rawRow.isFraud),

      // System fields
      status: "PENDING",
      createdAt: new Date(),
    };

    // Build identity data (if any identity columns exist)
    const hasIdentityData =
      Object.keys(ID_COLUMNS).some((col) => rawRow[col] !== undefined) ||
      rawRow.DeviceType !== undefined ||
      rawRow.DeviceInfo !== undefined;

    const identity = hasIdentityData
      ? {
          id_01: toFloat(rawRow.id_01),
          id_02: toFloat(rawRow.id_02),
          id_03: toFloat(rawRow.id_03),
          id_04: toFloat(rawRow.id_04),
          id_05: toFloat(rawRow.id_05),
          id_06: toFloat(rawRow.id_06),
          id_07: toFloat(rawRow.id_07),
          id_08: toFloat(rawRow.id_08),
          id_09: toFloat(rawRow.id_09),
          id_10: toFloat(rawRow.id_10),
          id_11: toFloat(rawRow.id_11),
          id_12: toString(rawRow.id_12),
          id_13: toFloat(rawRow.id_13),
          id_14: toFloat(rawRow.id_14),
          id_15: toString(rawRow.id_15),
          id_16: toString(rawRow.id_16),
          id_17: toFloat(rawRow.id_17),
          id_18: toFloat(rawRow.id_18),
          id_19: toFloat(rawRow.id_19),
          id_20: toFloat(rawRow.id_20),
          id_21: toFloat(rawRow.id_21),
          id_22: toFloat(rawRow.id_22),
          id_23: toString(rawRow.id_23),
          id_24: toFloat(rawRow.id_24),
          id_25: toFloat(rawRow.id_25),
          id_26: toFloat(rawRow.id_26),
          id_27: toString(rawRow.id_27),
          id_28: toString(rawRow.id_28),
          id_29: toString(rawRow.id_29),
          id_30: toString(rawRow.id_30),
          id_31: toString(rawRow.id_31),
          id_32: toFloat(rawRow.id_32),
          id_33: toString(rawRow.id_33),
          id_34: toString(rawRow.id_34),
          id_35: toString(rawRow.id_35),
          id_36: toString(rawRow.id_36),
          id_37: toString(rawRow.id_37),
          id_38: toString(rawRow.id_38),
          DeviceType: toString(rawRow.DeviceType),
          DeviceInfo: toString(rawRow.DeviceInfo),
        }
      : null;

    // Build V features data (if any V columns exist)
    const hasVData = Object.keys(V_COLUMNS).some(
      (col) => rawRow[col] !== undefined
    );

    const vFeatures = hasVData
      ? {
          V1: toFloat(rawRow.V1),
          V3: toFloat(rawRow.V3),
          V4: toFloat(rawRow.V4),
          V6: toFloat(rawRow.V6),
          V8: toFloat(rawRow.V8),
          V11: toFloat(rawRow.V11),
          V13: toFloat(rawRow.V13),
          V14: toFloat(rawRow.V14),
          V17: toFloat(rawRow.V17),
          V20: toFloat(rawRow.V20),
          V23: toFloat(rawRow.V23),
          V26: toFloat(rawRow.V26),
          V27: toFloat(rawRow.V27),
          V30: toFloat(rawRow.V30),
          V36: toFloat(rawRow.V36),
          V37: toFloat(rawRow.V37),
          V40: toFloat(rawRow.V40),
          V41: toFloat(rawRow.V41),
          V44: toFloat(rawRow.V44),
          V47: toFloat(rawRow.V47),
          V48: toFloat(rawRow.V48),
          V54: toFloat(rawRow.V54),
          V56: toFloat(rawRow.V56),
          V59: toFloat(rawRow.V59),
          V62: toFloat(rawRow.V62),
          V65: toFloat(rawRow.V65),
          V67: toFloat(rawRow.V67),
          V68: toFloat(rawRow.V68),
          V70: toFloat(rawRow.V70),
          V76: toFloat(rawRow.V76),
          V78: toFloat(rawRow.V78),
          V80: toFloat(rawRow.V80),
          V82: toFloat(rawRow.V82),
          V86: toFloat(rawRow.V86),
          V88: toFloat(rawRow.V88),
          V89: toFloat(rawRow.V89),
          V91: toFloat(rawRow.V91),
          V107: toFloat(rawRow.V107),
          V108: toFloat(rawRow.V108),
          V111: toFloat(rawRow.V111),
          V115: toFloat(rawRow.V115),
          V117: toFloat(rawRow.V117),
          V120: toFloat(rawRow.V120),
          V121: toFloat(rawRow.V121),
          V123: toFloat(rawRow.V123),
          V124: toFloat(rawRow.V124),
          V127: toFloat(rawRow.V127),
          V129: toFloat(rawRow.V129),
          V130: toFloat(rawRow.V130),
          V136: toFloat(rawRow.V136),
          V138: toFloat(rawRow.V138),
          V139: toFloat(rawRow.V139),
          V142: toFloat(rawRow.V142),
          V147: toFloat(rawRow.V147),
          V156: toFloat(rawRow.V156),
          V160: toFloat(rawRow.V160),
          V162: toFloat(rawRow.V162),
          V165: toFloat(rawRow.V165),
          V166: toFloat(rawRow.V166),
          V169: toFloat(rawRow.V169),
          V171: toFloat(rawRow.V171),
          V173: toFloat(rawRow.V173),
          V175: toFloat(rawRow.V175),
          V176: toFloat(rawRow.V176),
          V178: toFloat(rawRow.V178),
          V180: toFloat(rawRow.V180),
          V182: toFloat(rawRow.V182),
          V185: toFloat(rawRow.V185),
          V187: toFloat(rawRow.V187),
          V188: toFloat(rawRow.V188),
          V198: toFloat(rawRow.V198),
          V203: toFloat(rawRow.V203),
          V205: toFloat(rawRow.V205),
          V207: toFloat(rawRow.V207),
          V209: toFloat(rawRow.V209),
          V210: toFloat(rawRow.V210),
          V215: toFloat(rawRow.V215),
          V218: toFloat(rawRow.V218),
          V220: toFloat(rawRow.V220),
          V221: toFloat(rawRow.V221),
          V223: toFloat(rawRow.V223),
          V224: toFloat(rawRow.V224),
          V226: toFloat(rawRow.V226),
          V228: toFloat(rawRow.V228),
          V229: toFloat(rawRow.V229),
          V234: toFloat(rawRow.V234),
          V235: toFloat(rawRow.V235),
          V238: toFloat(rawRow.V238),
          V240: toFloat(rawRow.V240),
          V250: toFloat(rawRow.V250),
          V252: toFloat(rawRow.V252),
          V253: toFloat(rawRow.V253),
          V257: toFloat(rawRow.V257),
          V258: toFloat(rawRow.V258),
          V260: toFloat(rawRow.V260),
          V261: toFloat(rawRow.V261),
          V264: toFloat(rawRow.V264),
          V266: toFloat(rawRow.V266),
          V267: toFloat(rawRow.V267),
          V271: toFloat(rawRow.V271),
          V274: toFloat(rawRow.V274),
          V277: toFloat(rawRow.V277),
          V281: toFloat(rawRow.V281),
          V283: toFloat(rawRow.V283),
          V284: toFloat(rawRow.V284),
          V285: toFloat(rawRow.V285),
          V286: toFloat(rawRow.V286),
          V289: toFloat(rawRow.V289),
          V291: toFloat(rawRow.V291),
          V294: toFloat(rawRow.V294),
          V296: toFloat(rawRow.V296),
          V297: toFloat(rawRow.V297),
          V301: toFloat(rawRow.V301),
          V303: toFloat(rawRow.V303),
          V305: toFloat(rawRow.V305),
          V307: toFloat(rawRow.V307),
          V309: toFloat(rawRow.V309),
          V310: toFloat(rawRow.V310),
          V314: toFloat(rawRow.V314),
          V320: toFloat(rawRow.V320),
        }
      : null;

    return {
      valid: true,
      data: {
        transaction,
        identity,
        vFeatures,
      },
    };
  } catch (err) {
    return { valid: false, error: `Validation error: ${err.message}` };
  }
}

// Type conversion helpers - preserve null/undefined properly
const toBoolean = (value) => {
  if (value === null || value === undefined || value === "") return false;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value === 1;
  if (typeof value === "string") return /^(true|yes|1|y|t)$/i.test(value);
  return false;
};

const toFloat = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const f = parseFloat(value);
  return isNaN(f) ? null : f;
};

const toInt = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const i = parseInt(value, 10);
  return isNaN(i) ? null : i;
};

const toBigInt = (value) => {
  if (value === null || value === undefined || value === "") return null;
  try {
    return BigInt(Math.floor(Number(value)));
  } catch {
    return null;
  }
};

const toDecimal = (value) => {
  if (value === null || value === undefined || value === "") return 0;
  const f = parseFloat(value);
  return isNaN(f) ? 0 : f;
};

const toString = (value) => {
  if (value === null || value === undefined || value === "") return null;
  return String(value).trim();
};

const toChar = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const str = String(value).trim();
  return str.length > 0 ? str.charAt(0) : null;
};
