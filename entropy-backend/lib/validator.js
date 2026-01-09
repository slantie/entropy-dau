import { v4 as uuidv4 } from "uuid";

export function validateRow(rawRow) {
  try {
    if (!rawRow.timestamp) return { valid: false, error: "Missing timestamp" };
    if (!rawRow.sender_account)
      return { valid: false, error: "Missing sender_account" };
    if (!rawRow.receiver_account)
      return { valid: false, error: "Missing receiver_account" };
    if (!rawRow.amount_ngn || rawRow.amount_ngn <= 0)
      return { valid: false, error: "Invalid amount_ngn" };

    const normalized = {
      id: uuidv4(),
      timestamp: new Date(rawRow.timestamp),
      senderAccount: BigInt(rawRow.sender_account),
      receiverAccount: BigInt(rawRow.receiver_account),
      amountNgn: parseFloat(rawRow.amount_ngn),
      transactionType: String(rawRow.transaction_type || "UNKNOWN"),
      merchantCategory: String(rawRow.merchant_category || "UNKNOWN"),
      paymentChannel: String(rawRow.payment_channel || "UNKNOWN"),
      location: String(rawRow.location || "UNKNOWN"),
      deviceUsed: String(rawRow.device_used || "UNKNOWN"),
      fraudType: rawRow.fraud_type ? String(rawRow.fraud_type) : null,
      ipGeoRegion: String(rawRow.ip_geo_region || "UNKNOWN"),

      txnHour: parseInt(rawRow.txn_hour || 0),
      isWeekend: toBoolean(rawRow.is_weekend),
      isSalaryWeek: toBoolean(rawRow.is_salary_week),
      isNightTxn: toBoolean(rawRow.is_night_txn),

      timeSinceLastTransaction: toFloat(rawRow.time_since_last_transaction),
      spendingDeviationScore: parseFloat(rawRow.spending_deviation_score || 0),
      velocityScore: parseInt(rawRow.velocity_score || 0),
      geoAnomalyScore: parseFloat(rawRow.geo_anomaly_score || 0),
      bvnLinked: toBoolean(rawRow.bvn_linked),
      newDeviceTransaction: toBoolean(rawRow.new_device_transaction),
      senderPersona: String(rawRow.sender_persona || "UNKNOWN"),
      geospatialVelocityAnomaly: toBoolean(rawRow.geospatial_velocity_anomaly),

      deviceSeenCount: parseInt(rawRow.device_seen_count || 0),
      isDeviceShared: toBoolean(rawRow.is_device_shared),
      ipSeenCount: parseInt(rawRow.ip_seen_count || 0),
      isIpShared: toBoolean(rawRow.is_ip_shared),

      userTxnCountTotal: parseInt(rawRow.user_txn_count_total || 0),
      userAvgTxnAmt: parseFloat(rawRow.user_avg_txn_amt || 0),
      userStdTxnAmt: parseFloat(rawRow.user_std_txn_amt || 0),
      userTxnFrequency24h: parseInt(rawRow.user_txn_frequency_24h || 0),
      userTopCategory: String(rawRow.user_top_category || "UNKNOWN"),

      txnCountLast1h: parseInt(rawRow.txn_count_last_1h || 0),
      txnCountLast24h: parseInt(rawRow.txn_count_last_24h || 0),
      totalAmountLast1h: parseFloat(rawRow.total_amount_last_1h || 0),
      timeSinceLast: toFloat(rawRow.time_since_last),
      avgGapBetweenTxns: toFloat(rawRow.avg_gap_between_txns),

      merchantFraudRate: clamp(parseFloat(rawRow.merchant_fraud_rate || 0)),
      channelRiskScore: clamp(parseFloat(rawRow.channel_risk_score || 0)),
      personaFraudRisk: clamp(parseFloat(rawRow.persona_fraud_risk || 0)),
      locationFraudRisk: clamp(parseFloat(rawRow.location_fraud_risk || 0)),

      isFraud: toBoolean(rawRow.is_fraud),
      status: "PENDING",
      createdAt: new Date(),
    };

    return { valid: true, data: normalized };
  } catch (err) {
    return { valid: false, error: err.message };
  }
}

const toBoolean = (value) => {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") return /^(true|yes|1|y)$/i.test(value);
  return !!value;
};

const toFloat = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const f = parseFloat(value);
  return isNaN(f) ? null : f;
};

const clamp = (value) => Math.max(0, Math.min(1, value));
