// Connect to your MongoDB instance first in mongosh, usually with the `mongo` command-line tool
// Assuming you are connected to the `config` database
const batchSize = 1000;
const nss = "finact-bigiddb.collections_metadata_1"
const dbConfig = db.getSiblingDB('config');

let cursor = dbConfig.rangeDeletions.find({"nss": nss}).batchSize(batchSize);

if (!cursor.hasNext()) {
    print(`Could not identify any range deletion documents for ${nss} in config`)
}

while (cursor.hasNext()) {
// Iterate over each deletion entry

    cursor.forEach(entry => {
        const collectionUuid = entry.collectionUuid;
        const range = entry.range;
        let collectionName = '';

        // Get all collections info assumes we use the finact-bigiddb database
        db.getSiblingDB('finact-bigiddb').getCollectionInfos().forEach(collectionInfo => {
            if (collectionInfo.info.uuid == collectionUuid ) {
                collectionName = collectionInfo.name;
            }
        });

        // Make sure the collection name is found
        if (!collectionName) {
            print('Collection with UUID', collectionUuid, 'not found.');

        } else {
            // Formulate the query based on the range
            const query = {
                "fullyQualifiedName": {
                    $gt: range.min.fullyQualifiedName,
                    $lt: range.max.fullyQualifiedName,
                }
            };
        
            // Perform the delete operation
            const dbCollection = db.getSiblingDB("finact-bigiddb").getCollection(collectionName);
            const result = dbCollection.deleteMany(query);
            const now = new Date();
    
            print(`${now} - Deleted ${result.deletedCount} documents from ${collectionName} for range.min:`, JSON.stringify(range));
        }})
    } 
    

