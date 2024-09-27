package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	mongoURI := os.Getenv("MONGODB_URI")
	if mongoURI == "" {
		log.Fatal("MONGODB_URI not set in .env file")
	}

	for {
		checkConnection(mongoURI)
		time.Sleep(10 * time.Second)
	}
}

func checkConnection(uri string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientOpts := options.Client().ApplyURI(uri)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Printf("Failed to connect to MongoDB: %v\n", err)
		return
	}
	defer client.Disconnect(ctx)

	// Test connection
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Printf("Failed to ping MongoDB: %v\n", err)
		return
	}

	now := time.Now().Format(time.RFC3339)
	fmt.Printf("\n--- Connection Check at %s ---\n", now)
	fmt.Println("Successfully connected to MongoDB")

	// Print connection information
	fmt.Println("Connection Information:")
	serverStatus, err := client.Database("admin").RunCommand(ctx, bson.D{{"serverStatus", 1}}).DecodeBytes()
	if err != nil {
		log.Printf("Failed to get server status: %v\n", err)
		return
	}
	fmt.Printf("Server version: %v\n", serverStatus.Lookup("version").StringValue())
	fmt.Printf("Connection type: %v\n", serverStatus.Lookup("transportSecurity", "type").StringValue())

	// Print cluster topology
	fmt.Println("Cluster Topology:")
	topology, err := client.Database("admin").RunCommand(ctx, bson.D{{"isMaster", 1}}).DecodeBytes()
	if err != nil {
		log.Printf("Failed to get cluster topology: %v\n", err)
		return
	}
	fmt.Printf("Is master: %v\n", topology.Lookup("ismaster").Boolean())
	if hosts, ok := topology.Lookup("hosts").ArrayOK(); ok {
		fmt.Println("Hosts:")
		for _, host := range hosts {
			fmt.Printf("  - %s\n", host.StringValue())
		}
	}
	if secondaries, ok := topology.Lookup("secondaries").ArrayOK(); ok {
		fmt.Println("Secondaries:")
		for _, secondary := range secondaries {
			fmt.Printf("  - %s\n", secondary.StringValue())
		}
	}

	// Print read preference
	readPref, _ := client.ReadPreference()
	fmt.Printf("Read Preference: %v\n", readPref)

	// Print write concern
	writeConcern := client.WriteConcern()
	if writeConcern != nil {
		fmt.Printf("Write Concern: %+v\n", writeConcern)
	}

	fmt.Println("--- End of Connection Check ---")
}
