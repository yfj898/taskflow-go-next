package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/yfj898/taskflow-go/internal/repository"
	"github.com/yfj898/taskflow-go/internal/schemamigrate"
)

func main() {
	phase := flag.String("phase", "status", "status, expand, contract, workflow-status, workflow-expand, verification-status, verification-expand")
	oldWritersStopped := flag.Bool("ack-old-writers-stopped", false, "required for contract; do not use before draining every old application writer")
	flag.Parse()
	dsn := os.Getenv("TASKFLOW_SCHEMA_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "TASKFLOW_SCHEMA_DSN is required (must point to the intended MySQL schema)")
		os.Exit(2)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if *phase == "verification-status" || *phase == "verification-expand" {
		appDB, err := repository.NewMySQL(dsn, 5, 2, true)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		migrator := repository.NewTaskRepository(appDB)
		if *phase == "verification-expand" {
			err = migrator.MigrateVerificationExplicit()
		} else {
			err = migrator.VerifyVerificationSchema()
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "verification schema:", err)
			os.Exit(1)
		}
		fmt.Println(`{"verification_schema_ready":true}`)
		return
	}
	if *phase == "workflow-status" || *phase == "workflow-expand" {
		appDB, err := repository.NewMySQL(dsn, 5, 2, true)
		if err != nil {
			fmt.Fprintln(os.Stderr, "workflow schema db:", err)
			os.Exit(1)
		}
		migrator := repository.NewTaskRepository(appDB)
		if *phase == "workflow-expand" {
			err = migrator.MigrateWorkflowSchemaExplicit()
		} else {
			err = migrator.VerifyManagedWorkflowSchema()
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "managed workflow schema not ready:", err)
			os.Exit(1)
		}
		fmt.Println(`{"workflow_schema_managed_ready": true}`)
		return
	}
	var status schemamigrate.Status
	if *phase == "status" {
		status, err = schemamigrate.Inspect(ctx, db)
	} else {
		status, err = schemamigrate.Apply(ctx, db, *phase, *oldWritersStopped)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "schema migration failed:", err)
		os.Exit(1)
	}
	output, _ := json.MarshalIndent(status, "", "  ")
	fmt.Println(string(output))
}
