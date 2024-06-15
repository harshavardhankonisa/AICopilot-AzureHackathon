require('dotenv').config();
const { MongoClient } = require('mongodb');
const { OpenAIClient, AzureKeyCredential} = require("@azure/openai");

async function ragWithVectorsearch(db, collectionName, question, numResults=3) {
    //A system prompt describes the responsibilities, instructions, and persona of the AI.
    const systemPrompt = `
        You are a helpful, fun and friendly sales assistant for Cosmic Works, a bicycle and bicycle accessories store.
        Your name is Cosmo.
        You are designed to answer questions about the products that Cosmic Works sells.
        
        Only answer questions related to the information provided in the list of products below that are represented
        in JSON format.
        
        If you are asked a question that is not in the list, respond with "I don't know."
        
        List of products:
    `;
    const collection = db.collection(collectionName);
    //generate vector embeddings for the incoming question
    const queryEmbedding = await generateEmbeddings(question);
    //perform vector search and return the results
    results = await vectorSearch(db, collectionName, question, numResults);
    productList = "";
    //remove contentVector from the results, create a string of the results for the prompt
    for (const result of results) {
        delete result['document']['contentVector'];
        productList += JSON.stringify(result['document']) + "\n\n";
    }

    //assemble the prompt for the large language model (LLM)
    const formattedPrompt = systemPrompt + productList;
    //prepare messages for the LLM call, TODO: if message history is desired, add them to this messages array
    const messages = [
        {
            "role": "system",
            "content": formattedPrompt
        },
        {
            "role": "user",
            "content": question
        }
    ];
    const completionsDeploymentName = "gpt-4";
    const aoaiClient = new OpenAIClient(process.env.AZURE_OPENAI_API_INSTANCE_NAME, new AzureKeyCredential(process.env.AZURE_OPENAI_API_KEY));
    //call the Azure OpenAI model to get the completion and return the response
    const completion = await aoaiClient.getChatCompletions(completionsDeploymentName, messages);
    return completion.choices[0].message.content;
}

const startVectorSearchInRAG = async(client) => {
    //RAG with vector search for the top 3 most relevant products
    const db = client.db('cosmic_works');
    console.log(await ragWithVectorsearch(db, 'products', 'What are the names and skus of some of the bikes you have?', 3));
}

async function vectorSearch(db, collectionName, query, numResults = 3) {
    const collection = db.collection(collectionName);
    // generate the embedding for incoming question
    const queryEmbedding = await generateEmbeddings(query);
    
    const pipeline = [
        {
            '$search': {
                "cosmosSearch": {
                    "vector": queryEmbedding,
                    "path": "contentVector",
                    "k": numResults
                },
                "returnStoredSource": true
            }
        },
        { '$project': { 'similarityScore': { '$meta': 'searchScore' }, 'document': '$$ROOT' } }
    ];
    
    //perform vector search and return the results as an array
    const results = await collection.aggregate(pipeline).toArray();
    return results;
}

function printProductSearchResult(result) {  
    // Print the search result document in a readable format  
    console.log(`Similarity Score: ${result['similarityScore']}`);  
    console.log(`Name: ${result['document']['name']}`);  
    console.log(`Category: ${result['document']['categoryName']}`);  
    console.log(`SKU: ${result['document']['sku']}`);  
    console.log(`_id: ${result['document']['_id']}\n`);  
}

const startVectorSearch = async(client) => {
    const db = client.db('cosmic_works')
    const searchResults = await vectorSearch(db, 'products', 'What products do you have that are yellow?');    
    searchResults.forEach(printProductSearchResult);
}

async function addCollectionContentVectorField(db, collectionName) {
    const collection = db.collection(collectionName); 
    const docs = await collection.find({}).toArray();
    const bulkOperations = [];
    console.log(`Generating content vectors for ${docs.length} documents in ${collectionName} collection`);
    for (let i=0; i<docs.length; i++) {
        const doc = docs[i];
        // do not include contentVector field in the content to be embedded
        if ('contentVector' in doc) {
            delete doc['contentVector'];
        }
        const content = JSON.stringify(doc);
        const contentVector = await generateEmbeddings(content);
        bulkOperations.push({
            updateOne: {
                filter: { '_id': doc['_id'] },
                update: { '$set': { 'contentVector': contentVector } },
                upsert: true
            }
        });
        //output progress every 25 documents
        if ((i+1) % 25 === 0 || i === docs.length-1) {          
            console.log(`Generated ${i+1} content vectors of ${docs.length} in the ${collectionName} collection`);
        }
    }
    if (bulkOperations.length > 0) {
        console.log(`Persisting the generated content vectors in the ${collectionName} collection using bulkWrite upserts`);
        await collection.bulkWrite(bulkOperations);
        console.log(`Finished persisting the content vectors to the ${collectionName} collection`);
    }

    //check to see if the vector index already exists on the collection
    console.log(`Checking if vector index exists in the ${collectionName} collection`)
    const vectorIndexExists = await collection.indexExists('VectorSearchIndex');
    if (!vectorIndexExists) {
        await db.command({
            "createIndexes": collectionName,
            "indexes": [
            {
                "name": "VectorSearchIndex",
                "key": {
                "contentVector": "cosmosSearch"
                },
                "cosmosSearchOptions": {                  
                "kind": "vector-ivf",
                "numLists": 1,
                "similarity": "COS",
                "dimensions": 1536
                }
            }
            ]
        });
        console.log(`Created vector index on contentVector field on ${collectionName} collection`);
    }
    else {
        console.log(`Vector index already exists on contentVector field in the ${collectionName} collection`);
    }
}

const addCollectionContentVectorFieldStarter = async(client) => {
    const db = client.db('cosmic_works');
    // await addCollectionContentVectorField(db, 'products');
    await addCollectionContentVectorField(db, 'customers');
    // await addCollectionContentVectorField(db, 'sales');
}

async function generateEmbeddings(text) {
    // const embeddingsDeploymentName = "embeddings";
    const embeddingsDeploymentName = "text-embedding-ada-002";
    const completionsDeploymentName = "completions";
    const aoaiClient = new OpenAIClient(process.env.AZURE_OPENAI_API_INSTANCE_NAME, new AzureKeyCredential(process.env.AZURE_OPENAI_API_KEY));
    const embeddings = await aoaiClient.getEmbeddings(embeddingsDeploymentName, text);
    await new Promise(resolve => setTimeout(resolve, 500));
    return embeddings.data[0].embedding;
}

const testVectorSearch = async() => {
    console.log(await generateEmbeddings("Hello, world!"));
}

const loadingSalesData = async(client) => {
    // Load customer and sales data
    const db = client.db('cosmic_works');
    console.log('Retrieving combined Customer/Sales data');
    const customerCollection = db.collection('customers');
    const salesCollection = db.collection('sales');
    const custSalesRawData = "https://cosmosdbcosmicworks.blob.core.windows.net/cosmic-works-small/customer.json";
    const custSalesData = (await (await fetch(custSalesRawData)).json()).map(custSales => cleanData(custSales));

    console.log("Split customer and sales data");
    const customerData = custSalesData.filter(cust => cust["type"] === "customer");
    const salesData = custSalesData.filter(sales => sales["type"] === "salesOrder");

    console.log("Loading customer data");
    await customerCollection.deleteMany({});
    result = await customerCollection.insertMany(customerData);
    console.log(`${result.insertedCount} customers inserted`);

    console.log("Loading sales data");
    await salesCollection.deleteMany({});
    result = await salesCollection.insertMany(salesData);
    console.log(`${result.insertedCount} sales inserted`);
}

function cleanData(obj) {
    cleaned =  Object.fromEntries(
        Object.entries(obj).filter(([key, _]) => !key.startsWith('_'))
    );
    cleaned["_id"] = cleaned["id"];
    delete cleaned["id"];
    return cleaned;
}

const loadingProductData = async(client) => {
    console.log('Loading product data')
    const db = client.db('cosmic_works');
    const productCollection = db.collection('products');
    // await productCollection.deleteMany({});
    
    const productRawData = "https://cosmosdbcosmicworks.blob.core.windows.net/cosmic-works-small/product.json";
    const productData = (await (await fetch(productRawData)).json()).map(prod => cleanData(prod));
    var result = await productCollection.bulkWrite(
        productData.map((product) => ({
            insertOne: {
                document: product
            }
        }))
    );
    console.log(`${result.insertedCount} products inserted`);
}

const dropTheDatabase = async(client) => {
    const db = client.db('cosmic_works');
    await db.dropDatabase();
}

const findAllItemsFromDatabase = async(client) => {
    const db = client.db('cosmic_works');
    const products = db.collection('products');
    const allProducts = await products.find({}).toArray();
    console.log("All the products are listed here ",allProducts);
}

const insertMultipleItemsToDatabase = async(client) => {
    const db = client.db('cosmic_works');
    const products = db.collection('products');
    const productsToInsert = [
        {
            _id: "2BA4A26C-A8DB-4645-BEB9-F7D42F50262E",    
            categoryId: "56400CF3-446D-4C3F-B9B2-68286DA3BB99", 
            categoryName: "Bikes, Mountain Bikes", 
            sku:"BK-M18S-42",
            name: "Mountain-100 Silver, 42",
            description: 'The product called "Mountain-500 Silver, 42"',
            price: 742.42
        },
        {
            _id: "027D0B9A-F9D9-4C96-8213-C8546C4AAE71",    
            categoryId: "26C74104-40BC-4541-8EF5-9892F7F03D72", 
            categoryName: "Components, Saddles", 
            sku: "SE-R581",
            name: "LL Road Seat/Saddle",
            description: 'The product called "LL Road Seat/Saddle"',
            price: 27.12
        },
        {
            _id: "4E4B38CB-0D82-43E5-89AF-20270CD28A04",
            categoryId: "75BF1ACB-168D-469C-9AA3-1FD26BB4EA4C",
            categoryName:  "Bikes, Touring Bikes",
            sku: "BK-T44U-60",
            name: "Touring-2000 Blue, 60",
            description: 'The product called Touring-2000 Blue, 60"',
            price: 1214.85
        },
        {
            _id: "5B5E90B8-FEA2-4D6C-B728-EC586656FA6D",
            categoryId: "75BF1ACB-168D-469C-9AA3-1FD26BB4EA4C",
            categoryName: "Bikes, Touring Bikes",
            sku: "BK-T79Y-60",
            name: "Touring-1000 Yellow, 60",
            description: 'The product called Touring-1000 Yellow, 60"',
            price: 2384.07
        },
        {
            _id: "7BAA49C9-21B5-4EEF-9F6B-BCD6DA7C2239",
            categoryId: "26C74104-40BC-4541-8EF5-9892F7F03D72",
            categoryName: "Components, Saddles",
            sku: "SE-R995",
            name: "HL Road Seat/Saddle",
            description: 'The product called "HL Road Seat/Saddle"',
            price: 52.64,
        }
    ]
    const result = await products.bulkWrite(
        productsToInsert.map((product) => ({
            insertOne: {
                document: product
            }
        }))
    );
    console.log("Bulk write operation result:", result);
}

const delteItemFromDatabase = async(client) => {
    const db = client.db('cosmic_works');
    const products = db.collection('products');
    const result = await products.deleteOne({ _id: '2BA4A26C-A8DB-4645-BEB9-F7D42F50262E' });
    console.log(`Number of documents deleted: ${result.deletedCount}`);
}

const updateItemFromDatabase = async(client) => {
    const db = client.db('cosmic_works');
    const products = db.collection('products');
    const options = { returnDocument: 'after' };
    const updated = await products.findOneAndUpdate(
        { _id: '2BA4A26C-A8DB-4645-BEB9-F7D42F50262E' },
        { $set: { price: 14242.42 } },
        options);
    console.log("Product data updated", updated);
}

const retriveItemFromDatabase = async(client) => {
    const db = client.db('cosmic_works');
    const products = db.collection('products');
    const product = await products.findOne({ _id: '2BA4A26C-A8DB-4645-BEB9-F7D42F50262E' });
    console.log("Found the product:", product);
}

const addItemToDatabase = async(client) => {
    const db = client.db('cosmic_works');
    const products = db.collection('products');
    const product = {
        _id: '2BA4A26C-A8DB-4645-BEB9-F7D42F50262E',
        categoryId: '56400CF3-446D-4C3F-B9B2-68286DA3BB99',
        categoryName: 'Bikes, Mountain Bikes',  
        sku: 'BK-M18S-42',  
        name: 'Mountain-100 Silver, 42',
        description: 'The product called "Mountain-500 Silver, 42"',
        price: 742.42             
    };
    const result = await products.insertOne(product);
    console.log(`A product was inserted with the _id: ${result.insertedId}`);
}

async function main(){
    const client = new MongoClient(process.env.AZURE_COSMOSDB_CONNECTION_STRING);

try {
    await client.connect();
    console.log('Connected to MongoDB');
    // await addItemToDatabase(client);
    // await retriveItemFromDatabase(client);
    // await updateItemFromDatabase(client);
    // await delteItemFromDatabase(client);
    // await insertMultipleItemsToDatabase(client);
    // await findAllItemsFromDatabase(client);
    // await dropTheDatabase(client);
    // await loadingProductData(client);
    // await loadingSalesData(client);
    // await testVectorSearch(client);
    // await addCollectionContentVectorFieldStarter(client);
    // await startVectorSearch(client);
    await startVectorSearchInRAG(client);
  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
    console.log('Disconnected from MongoDB');
  }
}
 
main();
