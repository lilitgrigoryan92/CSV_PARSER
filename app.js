const path=require("path")
const fs =require("fs")
const csv=require("csv-parser")
const { writeFile } = require('fs/promises');
const cluster = require('cluster');
const totalCPUs = require("os").cpus().length;

const dirPath = path.resolve(__dirname, "./files.csv");

function readFilesCsv(dirPath){
    return new Promise((resolve,reject)=>{
        if(!dirPath){
            reject(new Error("There arent directory"))
            return;
        }

        fs.readdir(dirPath,(err,files)=>{
            if(err){
                reject(err)
                return;
            }

            const csvFiles=files.filter((file)=>file.endsWith(".csv"))

           
        if(csvFiles.length===0){
            reject(new Error("There arent csv files"))
        return;
        }
const convertedDir=path.join(__dirname,"converted")
if(!fs.existsSync(convertedDir)){
    fs.mkdirSync(convertedDir)
}
const start=Date.now()
let count=0

csvFiles.forEach((file)=>{
    const input=path.join(dirPath,file)
    const output=path.join(convertedDir,file.replace(".csv",".json"))


const results = [];

fs.createReadStream(input)
  .pipe(csv())
  .on('data', (data) => {
    count+=1
    results.push(data)})
  .on('end', () => {
    writeFile(output,JSON.stringify(results,undefined,2))
  })
    .on("error",(error)=> reject(error))
  });

const end=Date.now()
const duration=end-start
resolve({count,duration})
        })
    })
    
}
if (cluster.isMaster) {
  console.log(`Master ${process.pid} is running`);

  for (let i = 0; i < totalCPUs; i++) {
    cluster.fork();
  }

  cluster.on("exit", (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} killed`);
    cluster.fork();
  })
}else {
    readFilesCsv(dirPath)
      .then(({ count, duration }) => {
        console.log(`Count is ${count}`);
        console.log(`Duration is ${duration} ms`);
      })
      .catch((error) => console.error(error.message));
  }



