import express from "express";
import { parseAndIngest } from "../lib/main.js";

export default function excelIngestRoute(upload) {
  const router = express.Router();

  router.post("/", upload.single("file"), async (req, res) => {
    let responseSent = false;

    try {
      if (!req.file) {
        responseSent = true;
        return res.status(400).json({ success: false, error: "No file" });
      }

      console.log(`File: ${req.file.originalname}`);

      const maxRows = req.query.maxRows ? parseInt(req.query.maxRows) : 10000;
      const result = await parseAndIngest(req.file.path, maxRows);
      responseSent = true;

      return res.json({
        success: true,
        ...result,
        message: `File processed: ${req.file.originalname}`,
      });
    } catch (err) {
      console.error("Error processing file:", err);
      res.status(500).json({ success: false, error: err.message });
      if (!responseSent) {
        responseSent = true;
        return res.status(500).json({ success: false, error: err.message });
      }
    }
  });

  return router;
}
