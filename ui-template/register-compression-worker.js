/**
 * Below code register a worker function to the window
 *
 * Because Chrome has a limitation when it won't allow Web Worker
 * to register from local files, thus here we create a internal URL
 * on the fly to bypass this.
 */

const workerFuncCode = CompressWorkerFunction.toString();
// We wrap around the actual workerFuncCode around
// Note the code is running in worker's context
// not in the main window
const wrappedCode = "onmessage = (e) => { " + workerFuncCode + "; CompressWorkerFunction(e); }";
let blob = new Blob([wrappedCode]);
let workerURL = window.URL.createObjectURL(blob);
const compressWorker = new Worker(workerURL);
if (compressWorker) {
    console.log("Compression worker registered");
}
compressWorker.onmessage = (e) => {
    console.log(e);
    window.notifyCompressionDone(e.data);
};

window.compressWorker = compressWorker;
// empty handler
window.notifyCompressionDone = (data) => {
    console.log(data);
};