(async () => {

  async function deleteManyTask({ targetNss, numOrphanDocs, rangeQuery  } = {}) {
  
    print(targetNss);
  
    let sessionOpts = {};
  
    let session = db.getMongo().startSession(sessionOpts);
  
    const [dbName, collName] = targetNss.split('.');
  
    let namespace = session.getDatabase(dbName).getCollection(collName);
  
    const shardKey = Object.keys(rangeQuery.min)[0];
    const minValue = rangeQuery.min[shardKey];
    const maxValue = rangeQuery.max[shardKey];
  
    const deleteQuery = {[shardKey]: {$gte: minValue, $lt: maxValue}};
  
    print(targetNss)
    print(`Processing range deletion task: ${JSON.stringify(rangeQuery)}`);
    print(`Expected orphan documents: ${numOrphanDocs}`);
  
    let deleteMany = async () => {
      return await namespace.deleteMany(deleteQuery)
        .deletedCount;
    };
  
      try {
        deletedCount = await deleteMany();
      } catch (error) {
        console.log(error);
      }
  
  
    return [deletedCount];
  }
  
  async function main() {
  
      // Script to delete orphans whose ranges are defined in config.rangeDeletions.
      // batchStart = 0 refers to the first document in config.rangeDeletions
      // batchEnd is exclusive, meaning it will process documents from batchStart to
      // batchEnd - 1
      const args = process.argv.slice(2);
      if (args.length < 6) {
        print(
          "Usage: mongosh <connection_string> --file <script.js> <dbName.collectionName> <batchStart> <batchEnd>"
        );
        quit(1);
      }
      const targetNss = args[3]; // nss we want to delete orphans from
      const batchStart = parseInt(args[4]); // Start index of the batch of range deletion tasks
      const batchEnd = parseInt(args[5]); // End index of the batch of range deletion tasks
  
      print(batchStart);
      print(batchEnd);
      if (
        isNaN(batchStart) ||
        isNaN(batchEnd) ||
        batchStart < 0 ||
        batchEnd <= batchStart
      ) {
        print(
          "Error: Invalid batch range. Ensure batchStart >= 0 and batchEnd > batchStart."
        );
        quit(1);
      }
      print("---------------------------");
      print("Orphan Documents Cleanup Script");
      print("---------------------------");
      print(`Target Namespace: ${targetNss}`);
      print(`Processing batch range: ${batchStart} to ${batchEnd}`);
  
      //// Gets data from the CSRS using the Config ServeR URI
      //const configClient = new Mongo(configServerUri);
      //const configDB = configClient.getDB("config");
      const configDB = db.getSiblingDB("config");
  
      
  
      // Fetch the batch of range deletions within the specified range
      const rangeDeletionsCursor = configDB.rangeDeletions
          .find({ nss: targetNss })
          .skip(batchStart)
          .limit(batchEnd - batchStart);
  
      // Fetch an array of range deletion tasks
    const rangeDeletions = await rangeDeletionsCursor.toArray();
  
    let totalDeletedDocs = 0;
    let processedTasks = 0;
  
  
    // Map each range deletion task to an async deletion operation
  const deletionTasks = rangeDeletions.map((range, index) => {
    return deleteManyTask({
      targetNss: targetNss,
      numOrphanDocs: range.numOrphanDocs,
      rangeQuery: range.range
    });
  });
  
  // Wait for all deletion tasks to complete in parallel
  const results = await Promise.all(deletionTasks);
  
  // Process results
  results.forEach((result, index) => {
    processedTasks++;
    const deletedCount = result[0]; // returned as [deletedCount]
    totalDeletedDocs += deletedCount;
    print(`Task ${index + 1}: Deleted ${deletedCount} documents.`);
  });
  
  print('---------------------------');
  print(`Script Execution Completed.`);
  print(`Total range deletion tasks processed: ${processedTasks}`);
  print(`Total documents deleted: ${totalDeletedDocs}`);
  print('---------------------------');
  }
  
  await main().finally(console.log);
  
  })();
  
  
  
  