const path = require("path");
const fs = require("fs");
const csv = require("csv-parser");
const { writeFile } = require('fs/promises');
const cluster = require('cluster');
const totalCPUs = require("os").cpus().length;

const dirPath = path.resolve(__dirname, "./files.csv");

function readFilesCsv(dirPath) {
  return new Promise((resolve, reject) => {
    if (!dirPath) {
      reject(new Error("There are no directories"));
      return;
    }

    fs.readdir(dirPath, (err, files) => {
      if (err) {
        reject(err);
        return;
      }

      const csvFiles = files.filter((file) => file.endsWith(".csv"));

      if (csvFiles.length === 0) {
        reject(new Error("There are no CSV files"));
        return;
      }

      const convertedDir = path.join(__dirname, "converted");
      if (!fs.existsSync(convertedDir)) {
        fs.mkdirSync(convertedDir);
      }

      const start = Date.now();
      let count = 0;

      csvFiles.forEach((file) => {
        const input = path.join(dirPath, file);
        const output = path.join(convertedDir, file.replace(".csv", ".json"));

        const results = [];

        fs.createReadStream(input)
          .pipe(csv())
          .on("data", (data) => {
            count += 1;
            results.push(data);
          })
          .on("end", () => {
            writeFile(output, JSON.stringify(results, undefined, 2));
          })
          .on("error", (error) => reject(error));
      });

      const end = Date.now();
      const duration = end - start;
      resolve({ count, duration });
    });
  });
}

if (cluster.isMaster) {
  console.log("Master is running");

  const workerCount = Math.min(totalCPUs, 1); 

  for (let i = 0; i < workerCount; i++) {
    const worker = cluster.fork();
    worker.on("message", (message) => {
      console.log( message);
    });
  }

  cluster.on("exit", (worker, code, signal) => {
    console.log("Worker  killed");
    cluster.fork();
  });
} else {
  readFilesCsv(dirPath)
    .then(({ count, duration }) => {
      console.log(`Count is ${count}`);
      console.log(`Duration is ${duration} ms`);
      process.send({count, duration}); 
    })
    .catch((error) => console.error(error.message));
}
