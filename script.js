// Script to delete orphans whose ranges are defined in config.rangeDeletions.
// batchStart = 0 refers to the first document in config.rangeDeletions
// batchEnd is exclusive, meaning it will process documents from batchStart to
// batchEnd - 1
const args = process.argv.slice(2);
if (args.length < 5) {
  print(
      'Usage: mongosh <connection_string> <script.js> <dbName.collectionName> <batchStart> <batchEnd>');
  quit(1);
}
const targetNss = args[2];             // nss we want to delete orphans from
const batchStart = parseInt(args[3]);  // Start index of the batch of range deletion tasks
const batchEnd = parseInt(args[4]);    // End index of the batch of range deletion tasks
if (isNaN(batchStart) || isNaN(batchEnd) || batchStart < 0 ||
    batchEnd <= batchStart) {
  print(
      'Error: Invalid batch range. Ensure batchStart >= 0 and batchEnd > batchStart.');
  quit(1);
}
print('---------------------------');
print('Orphan Documents Cleanup Script');
print('---------------------------');
print(`Target Namespace: ${targetNss}`);
print(`Processing batch range: ${batchStart} to ${batchEnd}`);
const configDB = db.getSiblingDB('config');
// Fetch the batch of range deletions within the specified range
const rangeDeletionsCursor = configDB.rangeDeletions.find({nss: targetNss})
                                 .skip(batchStart)
                                 .limit(batchEnd - batchStart);
let totalDeletedDocs = 0;
let processedTasks = 0;
while (rangeDeletionsCursor.hasNext()) {
  const range = rangeDeletionsCursor.next();
  processedTasks++;
  const numOrphanDocs = range.numOrphanDocs;
  const rangeQuery = range.range;
  const shardKey = Object.keys(rangeQuery.min)[0];
  const minValue = rangeQuery.min[shardKey];
  const maxValue = rangeQuery.max[shardKey];
  print(`Processing range deletion task ${processedTasks}: ${JSON.stringify(rangeQuery)}`);
  print(`Expected orphan documents: ${numOrphanDocs}`);
  const [dbName, collName] = targetNss.split('.');
  const collection = db.getSiblingDB(dbName).getCollection(collName);
  const deleteQuery = {[shardKey]: {$gte: minValue, $lt: maxValue}};
  try {
    const deleteResult = collection.deleteMany(deleteQuery);
    totalDeletedDocs += deleteResult.deletedCount;
    print(`Deleted ${deleteResult.deletedCount} documents.`);
  } catch (err) {
    print(`Error deleting documents from ${nss} for range id ${range._id}: ${
        err}`);
    quit(1);
  }
}
print('---------------------------');
print(`Script Execution Completed.`);
print(`Total range deletion tasks processed: ${processedTasks}`);
print(`Total documents deleted: ${totalDeletedDocs}`);
print('---------------------------');
