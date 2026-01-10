import axios from "axios";
import express from "express";
import pkg from "@prisma/client";
const { PrismaClient } = pkg;

const router = express.Router();
const prisma = new PrismaClient();

const PYTHON_ML_SERVER =
  process.env.PYTHON_ML_SERVER || "http://localhost:8000";

function serializeBigInt(obj) {
  return JSON.parse(
    JSON.stringify(obj, (key, value) =>
      typeof value === "bigint" ? value.toString() : value
    )
  );
}

function convertToMLFeatures(txn) {
  const features = {
    amount_ngn: txn.amountNgn || 0,
    receiver_account: Number(txn.receiverAccount) || 0,

    txn_hour: txn.txnHour || 0,
    txn_dayofweek: 0,
    is_weekend: txn.isWeekend ? 1 : 0,
    is_salary_week: txn.isSalaryWeek ? 1 : 0,
    is_night_txn: txn.isNightTxn ? 1 : 0,

    location: txn.location || 0,
    device_used: txn.deviceUsed || 0,
    ip_geo_region: txn.ipGeoRegion || 0,

    time_since_last_transaction: txn.timeSinceLastTransaction || 0,
    time_since_last: txn.timeSinceLast || 0,
    avg_gap_between_txns: txn.avgGapBetweenTxns || 0,

    spending_deviation_score: txn.spendingDeviationScore || 0,
    velocity_score: txn.velocityScore || 0,
    geo_anomaly_score: txn.geoAnomalyScore || 0,

    device_seen_count: txn.deviceSeenCount || 0,
    is_device_shared: txn.isDeviceShared ? 1 : 0,
    ip_seen_count: txn.ipSeenCount || 0,
    is_ip_shared: txn.isIpShared ? 1 : 0,
    new_device_transaction: txn.newDeviceTransaction ? 1 : 0,

    user_txn_count_total: txn.userTxnCountTotal || 0,
    user_avg_txn_amt: txn.userAvgTxnAmt || 0,
    user_std_txn_amt: txn.userStdTxnAmt || 0,
    user_txn_frequency_24h: txn.userTxnFrequency24h || 0,

    txn_count_last_1h: txn.txnCountLast1h || 0,
    txn_count_last_24h: txn.txnCountLast24h || 0,
    total_amount_last_1h: txn.totalAmountLast1h || 0,

    merchant_fraud_rate: txn.merchantFraudRate || 0,
    channel_risk_score: txn.channelRiskScore || 0,
    persona_fraud_risk: txn.personaFraudRisk || 0,
    location_fraud_risk: txn.locationFraudRisk || 0,

    bvn_linked: txn.bvnLinked ? 1 : 0,

    transaction_type_payment: txn.transactionType === "payment" ? 1 : 0,
    transaction_type_withdrawal: txn.transactionType === "withdrawal" ? 1 : 0,
    transaction_type_transfer: txn.transactionType === "transfer" ? 1 : 0,

    payment_channel_Mobile_App: txn.paymentChannel === "Mobile App" ? 1 : 0,
    payment_channel_Card: txn.paymentChannel === "Card" ? 1 : 0,
    payment_channel_USSD: txn.paymentChannel === "USSD" ? 1 : 0,

    merchant_category_Other_Transaction:
      txn.merchantCategory === "Other Transaction" ? 1 : 0,
    merchant_category_SPAR_Purchase:
      txn.merchantCategory === "SPAR Purchase" ? 1 : 0,

    sender_persona_Student: txn.senderPersona === "Student" ? 1 : 0,
    sender_persona_Trader: txn.senderPersona === "Trader" ? 1 : 0,

    user_top_category_Other_Transaction:
      txn.userTopCategory === "Other Transaction" ? 1 : 0,
    user_top_category_ATM_Withdrawal:
      txn.userTopCategory === "ATM Withdrawal" ? 1 : 0,
    user_top_category_Bet9ja_Stake:
      txn.userTopCategory === "Bet9ja Stake" ? 1 : 0,
    user_top_category_Airtime_Top_up_MTN: txn.userTopCategory?.includes(
      "Airtime"
    )
      ? 1
      : 0,
    user_top_category_Arik_Air_Flight: txn.userTopCategory?.includes("Arik")
      ? 1
      : 0,
  };

  features.composite_anomaly =
    ((features.spending_deviation_score || 0) +
      (features.geo_anomaly_score || 0) +
      (features.velocity_score || 0)) /
    3;

  features.amount_zscore =
    features.user_std_txn_amt > 0
      ? (features.amount_ngn - features.user_avg_txn_amt) /
        features.user_std_txn_amt
      : 0;

  features.velocity_anomaly =
    features.user_txn_frequency_24h > 0
      ? features.txn_count_last_1h / (features.user_txn_frequency_24h + 1)
      : 0;

  features.sharing_risk =
    (features.is_device_shared || 0) + (features.is_ip_shared || 0);

  features.risk_night_interaction =
    (features.channel_risk_score || 0) * (features.is_night_txn || 0);

  return features;
}

router.get("/seed", async (req, res) => {
  try {
    const count = await prisma.transaction.count();

    if (count === 0) return res.json([]);

    const skip = Math.max(0, Math.floor(Math.random() * (count - 5)));

    const transactions = await prisma.transaction.findMany({
      take: 1,
      skip: skip,
    });

    res.json(serializeBigInt(transactions));
  } catch (error) {
    console.error("Simulation Seed Error:", error);
    res.status(500).json({ error: "Failed to fetch seed data" });
  }
});

router.post("/analyze", async (req, res) => {
  try {
    const transactionData = req.body;

    console.log(`[Backend] Processing Transaction ID ${transactionData.id}...`);
    console.log(`[Backend] Transaction fields:`, Object.keys(transactionData));

    let mlResult = {
      riskScore: 0.5,
      prediction: "REVIEW",
      confidence: 0.0,
      top_features: [],
    };

    try {
      console.log(
        `[Backend] Sending to Python ML Server: ${PYTHON_ML_SERVER}/predict`
      );

      const features = convertToMLFeatures(transactionData);

      console.log(`[Backend] Extracted features:`, features);

      const payload = {
        transactionId: transactionData.id,
        features: features,
      };

      console.log(
        `[Backend] Sending payload to Python:`,
        JSON.stringify(payload)
      );

      const mlResponse = await axios.post(
        `${PYTHON_ML_SERVER}/predict`,
        payload,
        { timeout: 10000 }
      );

      mlResult = mlResponse.data;
      console.log(`[Backend] ML Response:`, mlResult);
    } catch (pythonError) {
      console.warn(
        `[Backend] Python server error (${pythonError.message}), using fallback`
      );
      console.warn(
        `[Backend] Python error details:`,
        pythonError.response?.data
      );
      mlResult = generateMockScore(transactionData);
    }

    const result = {
      transactionId: transactionData.id,
      riskScore: parseFloat(mlResult.riskScore.toFixed(4)),
      status: mapPredictionToStatus(mlResult.prediction || mlResult.status),
      prediction: mlResult.prediction,
      confidence: mlResult.confidence || 0,
      topFeatures: mlResult.top_features || [],
      processedAt: new Date().toISOString(),
    };

    console.log(`[Backend] Returning result:`, result);
    res.json(result);
  } catch (error) {
    console.error("Simulation Analyze Error:", error);
    res.status(500).json({ error: "Analysis failed", details: error.message });
  }
});

function mapPredictionToStatus(prediction) {
  if (!prediction) return "REVIEW";
  const pred = prediction.toUpperCase();
  if (pred.includes("FRAUD") || pred.includes("BLOCKED")) return "BLOCKED";
  if (pred.includes("SUSPICIOUS") || pred.includes("REVIEW")) return "REVIEW";
  return "APPROVED";
}

function generateMockScore(transactionData) {
  let riskScore = Math.random() * 0.3;
  if (transactionData.amountNgn > 500000) riskScore += 0.4;
  if (transactionData.isNightTxn) riskScore += 0.2;
  if (transactionData.isDeviceShared) riskScore += 0.2;
  riskScore = Math.min(0.99, riskScore);

  return {
    riskScore,
    prediction:
      riskScore > 0.8 ? "FRAUD" : riskScore > 0.5 ? "SUSPICIOUS" : "LEGITIMATE",
    confidence: 0.75,
    top_features: ["amount", "device_seen_count", "merchant_fraud_rate"],
  };
}

export default router;
