import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import multer from "multer";
import path from "path";
import { fileURLToPath } from "url";
import excelIngestRoute from "./routes/excelIngest.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

dotenv.config();

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const storage = multer.diskStorage({
  destination: (request, file, cb) => {
    cb(null, path.join(__dirname, "temp-uploads"));
  },
  filename: (request, file, cb) => {
    cb(null, file.originalname + "-" + Date.now());
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 200 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowed = [
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "application/vnd.ms-excel",
    ];
    if (allowed.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error("Only Excel files allowed"));
    }
  },
});

app.use("/api/upload-excel", excelIngestRoute(upload));

app.use("/health", (req, res) => {
  res.status(200).send("OK");
});

app.use((err, req, res, next) => {
  if (res.headersSent) {
    console.error(`Headers already sent:`, err.message);
    return next(err);
  }
  console.error(err.stack);
  res.status(500).json({ error: err.message });
});

export default app;
