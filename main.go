package main

import (
	"context"
	"fmt"
	"log"
	"net/smtp"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	lastConnectionStatus bool
	smtpHost             string
	smtpPort             string
	fromEmail            string
	toEmail              string
	password             string
	checkInterval        time.Duration
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	smtpHost = os.Getenv("SMTP_HOST")
	smtpPort = os.Getenv("SMTP_PORT")
	fromEmail = os.Getenv("FROM_EMAIL")
	toEmail = os.Getenv("TO_EMAIL")
	password = os.Getenv("EMAIL_PASSWORD")

	if smtpHost == "" || smtpPort == "" || fromEmail == "" || toEmail == "" || password == "" {
		log.Fatal("Email configuration is incomplete in .env file")
	}

	intervalStr := os.Getenv("CHECK_INTERVAL_SECONDS")
	if intervalStr == "" {
		intervalStr = "30" // Default to 30 seconds if not specified
	}
	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		log.Fatalf("Invalid CHECK_INTERVAL_SECONDS: %v", err)
	}
	checkInterval = time.Duration(interval) * time.Second
}

func main() {
	mongoURI := os.Getenv("MONGODB_URI")
	if mongoURI == "" {
		log.Fatal("MONGODB_URI not set in .env file")
	}

	log.Printf("Starting MongoDB connection monitor. Check interval: %v\n", checkInterval)

	for {
		checkConnection(mongoURI)
		time.Sleep(checkInterval)
	}
}

func checkConnection(uri string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientOpts := options.Client().ApplyURI(uri)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		handleConnectionFailure(fmt.Sprintf("Failed to connect to MongoDB: %v", err))
		return
	}
	defer client.Disconnect(ctx)

	// Test connection
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		handleConnectionFailure(fmt.Sprintf("Failed to ping MongoDB: %v", err))
		return
	}

	// Connection successful
	handleConnectionSuccess()

	now := time.Now().Format(time.RFC3339)
	fmt.Printf("\n--- Connection Check at %s ---\n", now)
	fmt.Println("Successfully connected to MongoDB")

	// Print connection information
	fmt.Println("Connection Information:")
	var serverStatus bson.M
	err = client.Database("admin").RunCommand(ctx, bson.D{{"serverStatus", 1}}).Decode(&serverStatus)
	if err != nil {
		log.Printf("Failed to get server status: %v\n", err)
		return
	}
	fmt.Printf("Server version: %v\n", serverStatus["version"])
	if transportSecurity, ok := serverStatus["transportSecurity"].(bson.M); ok {
		fmt.Printf("Connection type: %v\n", transportSecurity["type"])
	}

	// Print cluster topology
	fmt.Println("Cluster Topology:")
	var topology bson.M
	err = client.Database("admin").RunCommand(ctx, bson.D{{"isMaster", 1}}).Decode(&topology)
	if err != nil {
		log.Printf("Failed to get cluster topology: %v\n", err)
		return
	}
	fmt.Printf("Is master: %v\n", topology["ismaster"])
	if hosts, ok := topology["hosts"].(primitive.A); ok {
		fmt.Println("Hosts:")
		for _, host := range hosts {
			fmt.Printf("  - %v\n", host)
		}
	}
	if secondaries, ok := topology["secondaries"].(primitive.A); ok {
		fmt.Println("Secondaries:")
		for _, secondary := range secondaries {
			fmt.Printf("  - %v\n", secondary)
		}
	}

	// Print read preference
	fmt.Printf("Read Preference: %v\n", clientOpts.ReadPreference)

	// Print write concern
	fmt.Printf("Write Concern: %+v\n", clientOpts.WriteConcern)

	fmt.Println("--- End of Connection Check ---")
}

func handleConnectionFailure(errorMsg string) {
	if lastConnectionStatus {
		sendAlert("MongoDB Connection Failed", errorMsg)
		lastConnectionStatus = false
	}
	log.Println(errorMsg)
}

func handleConnectionSuccess() {
	if !lastConnectionStatus {
		sendAlert("MongoDB Connection Restored", "The connection to MongoDB has been restored.")
		lastConnectionStatus = true
	}
}

func sendAlert(subject, body string) {
	auth := smtp.PlainAuth("", fromEmail, password, smtpHost)
	to := []string{toEmail}
	msg := []byte(fmt.Sprintf("To: %s\r\nSubject: %s\r\n\r\n%s", toEmail, subject, body))

	err := smtp.SendMail(smtpHost+":"+smtpPort, auth, fromEmail, to, msg)
	if err != nil {
		log.Printf("Failed to send alert email: %v\n", err)
		return
	}

	log.Printf("Alert email sent: %s\n", subject)
}
