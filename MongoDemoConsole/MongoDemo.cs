using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

//minimum
using MongoDB.Bson;
using MongoDB.Driver;

//frequently
using MongoDB.Driver.Builders;
using MongoDB.Driver.GridFS;
using MongoDB.Driver.Linq;

//ExampleBatchInsert
//add reference System.Runtime.Serialization & System.ServiceModel.Web
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;

namespace MongoDemoConsole
{
    class MongoDemo
    {
        static void Main(string[] args)
        {
            try
            {
                Example();

                Pattern.ExampleBatchInsert();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message + Environment.NewLine + ex.StackTrace);
            }
            Console.WriteLine("Press any key to continue ...");
            Console.ReadKey();
        }

        //http://mongodb.github.io/mongo-csharp-driver/2.4/getting_started/quick_tour/
        public static void Example()
        {
            // Connect
            // http://mongodb.github.io/mongo-csharp-driver/2.4/reference/driver/authentication/
            var mongoClientSettings = new MongoClientSettings
            {
                Credentials = new[] { MongoCredential.CreateCredential("daviddb", "dbuser", "userpwd") },
                Server = new MongoServerAddress("10.1.31.63", 27017)
            };
            var database = new MongoClient(mongoClientSettings).GetDatabase("daviddb");
            //OR var database = new MongoClient("mongodb://dbuser:userpwd@10.1.31.63:27017/daviddb").GetDatabase("daviddb");

            // Get collection
            if (database.ListCollections(new ListCollectionsOptions { Filter = new BsonDocument("name", "AEntities") }).Any())
                database.DropCollection("AEntities");
            var collection = database.GetCollection<MyEntity>("AEntities");

            // Insert
            var entityInsert = new MyEntity { Name = "Tom" };
            collection.InsertOne(entityInsert);
            var id = entityInsert.Id; // Insert will set the Id if necessary (as it was in this example)

            // Find
            var sort = Builders<MyEntity>.Sort.Descending("id");
            var projection = Builders<MyEntity>.Projection.Exclude("_id"); // don’t need all the data contained in a document
            var entityFind = collection.Find(e => e.Id == id).Project(projection).Sort(sort).FirstOrDefault();

            // Update
            var filterUpdate = Builders<MyEntity>.Filter.Eq(e => e.Id, id) & Builders<MyEntity>.Filter.Eq(e => e.Name, "Tom");
            var update = Builders<MyEntity>.Update.Set(e => e.Name, "Jerry"); // update modifiers
            collection.UpdateOne(filterUpdate, update); // UpdateMany

            // Delete
            var filterDelete = Builders<MyEntity>.Filter.Eq(e => e.Id, id) & Builders<MyEntity>.Filter.Eq(e => e.Name, "Jerry");
            collection.DeleteMany(filterDelete); // DeleteOne
        }
    }

    public class MyEntity
    {
        // If your domain class is going to be used as the root document it MUST contain an Id field
        // or property (typically named Id although you can override that if necessary). Normally
        // the Id will be of type ObjectId, but there are no constraints on the type of this member.
        public ObjectId Id { get; set; }
        public string Name { get; set; }
    }

    public class Pattern
    {
        public ObjectId id { get; set; }
        public bool enabled { get; set; }
        public string name { get; set; }
        public string pattern { get; set; }
        public bool isRegEx { get; set; }
        public bool caseSensitive { get; set; }
        public bool blackList { get; set; }
        public bool multiLine { get; set; }

        public static void ExampleBatchInsert()
        {
            // Connect
            var database = new MongoClient("mongodb://dbuser:userpwd@10.1.31.63:27017/daviddb").GetDatabase("daviddb");

            if (database.ListCollections(new ListCollectionsOptions { Filter = new BsonDocument("name", "JsonTest") }).Any())
                database.DropCollection("JsonTest");
            var collection = database.GetCollection<Pattern>("JsonTest");

            //read file
            string[] strJson = null;
            using (StreamReader streamReader = new StreamReader("PatternsExample.json", Encoding.UTF8))
            {
                strJson = streamReader.ReadToEnd().Replace("\n", "").Replace("\t", "").Replace("},", "}~split~").Split(new string[1] { "~split~" }, StringSplitOptions.RemoveEmptyEntries);
            }

            //write to DB
            var toInsert = new List<Pattern>();
            for (int i = 0; i < strJson.Length; i++)
            {
                try
                {
                    toInsert.Add(DeserializeFromJSON<Pattern>(strJson[i], typeof(Pattern), Encoding.UTF8));
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message + Environment.NewLine + ex.StackTrace);
                }
            }
            collection.InsertMany(toInsert);
            collection.DeleteMany(Builders<Pattern>.Filter.Eq(e => e.blackList, true)); // remove partially
        }

        public static string SerializeToJSON(object obj, Type type, Encoding encode)
        {
            using (var stream = new MemoryStream())
            {
                new DataContractJsonSerializer(type).WriteObject(stream, obj);
                return encode.GetString(stream.ToArray());
            }
        }
        public static T DeserializeFromJSON<T>(string str, Type type, Encoding encode)
        {
            using (MemoryStream ms = new MemoryStream(encode.GetBytes(str)))
            {
                return (T)new DataContractJsonSerializer(type).ReadObject(ms);
            }
        }
    }
}

