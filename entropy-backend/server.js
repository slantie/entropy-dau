import app from "./app.js";
import pkg from "@prisma/client";
const { PrismaClient } = pkg;

const PORT = process.env.PORT || 5000;
const prisma = new PrismaClient();

process.on("SIGINT", async () => {
  console.log("\nShutting Down!");
  await prisma.$disconnect();
  process.exit(0);
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
  console.log(`http://localhost:${PORT}`);
});
