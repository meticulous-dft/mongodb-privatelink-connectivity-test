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
	logFile              *os.File
)

func init() {
	var err error
	logFile, err = os.OpenFile("mongodb_connection_monitor.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Failed to open log file:", err)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	log.Println("Starting application initialization")

	err = godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file:", err)
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

	log.Println("Application initialization complete")
}

func main() {
	defer logFile.Close()

	mongoURI := os.Getenv("MONGODB_URI")
	if mongoURI == "" {
		log.Fatal("MONGODB_URI not set in .env file")
	}

	log.Printf("Starting MongoDB connection monitor. Check interval: %v\n", checkInterval)
	log.Printf("MongoDB URI: %s\n", mongoURI) // Be cautious with logging sensitive information

	for {
		checkConnection(mongoURI)
		time.Sleep(checkInterval)
	}
}

func checkConnection(uri string) {
	log.Println("Starting connection check")

	ctx, cancel := context.WithTimeout(context.Background(), checkInterval)
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
	log.Printf("\n--- Connection Check at %s ---\n", now)

	log.Println("Successfully connected to MongoDB")

	// Print connection information
	log.Println("Connection Information:")
	var serverStatus bson.M
	err = client.Database("admin").RunCommand(ctx, bson.D{{"serverStatus", 1}}).Decode(&serverStatus)
	if err != nil {
		log.Printf("Failed to get server status: %v\n", err)
		return
	}
	log.Printf("Server version: %v\n", serverStatus["version"])
	if transportSecurity, ok := serverStatus["transportSecurity"].(bson.M); ok {
		log.Printf("Connection type: %v\n", transportSecurity["type"])
	}

	// Print cluster topology
	log.Println("Cluster Topology:")
	var topology bson.M
	err = client.Database("admin").RunCommand(ctx, bson.D{{"isMaster", 1}}).Decode(&topology)
	if err != nil {
		log.Printf("Failed to get cluster topology: %v\n", err)
		return
	}
	log.Printf("Is master: %v\n", topology["ismaster"])
	if hosts, ok := topology["hosts"].(primitive.A); ok {
		log.Println("Hosts:")
		for _, host := range hosts {
			log.Printf("  - %v\n", host)
		}
	}
	if secondaries, ok := topology["secondaries"].(primitive.A); ok {
		log.Println("Secondaries:")
		for _, secondary := range secondaries {
			log.Printf("  - %v\n", secondary)
		}
	}

	// Print read preference
	log.Printf("Read Preference: %v\n", clientOpts.ReadPreference)

	// Print write concern
	log.Printf("Write Concern: %+v\n", clientOpts.WriteConcern)

	log.Println("Connection check complete")
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
