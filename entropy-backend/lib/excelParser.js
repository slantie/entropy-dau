import XLSX from "xlsx";

export async function parseExcel(path) {
  console.log("Parsing Excel file at:", path);
  const workbook = XLSX.readFile(path);
  const worksheet = workbook.Sheets[workbook.SheetNames[0]];
  const transactions = XLSX.utils.sheet_to_json(worksheet);
  console.log(`${transactions.length} transactions Proccessed.`);
  return transactions;
}

export const COLUMNS = {
  timestamp: "timestamp",
  sender_account: "senderAccount",
  receiver_account: "receiverAccount",
  amount_ngn: "amountNgn",

  transaction_type: "transactionType",
  merchant_category: "merchantCategory",
  location: "location",
  device_used: "deviceUsed",
  payment_channel: "paymentChannel",
  fraud_type: "fraudType",
  ip_geo_region: "ipGeoRegion",

  txn_hour: "txnHour",
  is_weekend: "isWeekend",
  is_salary_week: "isSalaryWeek",
  is_night_txn: "isNightTxn",

  time_since_last_transaction: "timeSinceLastTransaction",
  spending_deviation_score: "spendingDeviationScore",
  velocity_score: "velocityScore",
  geo_anomaly_score: "geoAnomalyScore",
  bvn_linked: "bvnLinked",
  new_device_transaction: "newDeviceTransaction",
  sender_persona: "senderPersona",
  geospatial_velocity_anomaly: "geospatialVelocityAnomaly",
  
  device_seen_count: "deviceSeenCount",
  is_device_shared: "isDeviceShared",
  ip_seen_count: "ipSeenCount",
  is_ip_shared: "isIpShared",
  
  user_txn_count_total: "userTxnCountTotal",
  user_avg_txn_amt: "userAvgTxnAmt",
  user_std_txn_amt: "userStdTxnAmt",
  user_txn_frequency_24h: "userTxnFrequency24h",
  user_top_category: "userTopCategory",
  
  txn_count_last_1h: "txnCountLast1h",
  txn_count_last_24h: "txnCountLast24h",
  total_amount_last_1h: "totalAmountLast1h",
  time_since_last: "timeSinceLast",
  avg_gap_between_txns: "avgGapBetweenTxns",
  
  merchant_fraud_rate: "merchantFraudRate",
  channel_risk_score: "channelRiskScore",
  persona_fraud_risk: "personaFraudRisk",
  location_fraud_risk: "locationFraudRisk",
  
  is_fraud: "isFraud",
};
