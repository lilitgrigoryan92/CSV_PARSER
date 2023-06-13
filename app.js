const fs = require('fs');
const csv = require('csv-parser');
const path = require("path");
const { writeFile } = require('fs/promises');


const dirPath = path.resolve(__dirname, "./files.csv");

 function readCSVFiles(dirPath) {
  return new Promise((resolve, reject) => {
    if (!dirPath) {
      reject(new Error('There arent directory'));
      return;
    }

    fs.readdir(dirPath, (err, files) => {
      if (err) {
        reject(err);
        return;
      }

      const csvFiles = files.filter(file => file.endsWith('.csv'));

      if (csvFiles.length === 0) {
        reject(new Error('There arent csv file.'));
        return;
      }

      const converted = path.join(__dirname, 'converted');
      if (!fs.existsSync(converted)) {
        fs.mkdirSync(converted);
      }

      const start = Date.now();
      let count = 0;

      csvFiles.forEach(file => {
        const inputFilePath = path.join(dirPath, file);
        const outputFilePath = path.join(converted, file.replace('.csv', '.json'));

        const results = [];
        fs.createReadStream(inputFilePath)
          .pipe(csv())
          .on('data', (data) => {
            count+=1
            results.push(data);
          })
          .on('end', () => {
            writeFile(outputFilePath, JSON.stringify(results, undefined, 2))
          })
          .on('error', error => reject(error));
      });

      const end = Date.now();
      const duration = end - start;
      resolve({ count, duration });
    });
  });
}


readCSVFiles(dirPath)
  .then(({ count, duration }) => {
    console.log("Count is",count);
    console.log("Duration is",duration,"ms");
  })
  .catch(error => {
    console.error(error.message);
  });