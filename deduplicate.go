package main

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

func withTransaction(db *sql.DB, fn func(db *sql.Tx) error) error {
	// begin a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error beginning transaction: %s", err)
	}
	defer tx.Rollback() // rollback the transaction if it's not committed

	err = fn(tx) // Run action
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction: %s", err)
	}

	return nil
}

func fillTempTable(tx *sql.Tx, batch Batch) error {
	_, err := tx.Exec(`
        CREATE TEMP TABLE temp_logs (log_id UUID) ON COMMIT DROP;
        CREATE INDEX temp_logs_log_id_idx ON temp_logs USING HASH (log_id);
    `)
	if err != nil {
		return fmt.Errorf("error creating temp table: %s", err)
	}

	// prepare copy command
	stmt, _ := tx.Prepare(pq.CopyIn("temp_logs", "log_id"))
	for key := range batch {
		_, err := stmt.Exec(key)
		if err != nil {
			return fmt.Errorf("error creating COPY handler: %s", err)
		}
	}
	defer stmt.Close()

	// consolidate copy command
	result, err := stmt.Exec()
	if err != nil {
		return fmt.Errorf("error running COPY command: %s", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error checking affected COPY rows: %s", err)
	}

	fmt.Println("Rows inserted into temp table:", rows)
	return nil
}

func fillLogs(tx *sql.Tx, batch Batch) error {
	// prepare copy command
	stmt, err := tx.Prepare(pq.CopyIn("processed_logs", "log_id"))
	if err != nil {
		return fmt.Errorf("error creating COPY handler: %w", err)
	}

	for key := range batch {
		if _, err := stmt.Exec(key); err != nil {
			return fmt.Errorf("error creating COPY handler: %w", err)
		}
	}
	defer stmt.Close()

	// consolidate copy command
	result, err := stmt.Exec()
	if err != nil {
		return fmt.Errorf("error running COPY command: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error checking affected COPY rows: %w", err)
	}

	fmt.Println("Rows inserted into final table:", rows)
	return nil
}

func deduplicateBatch(tx *sql.Tx, batch Batch) (Batch, error) {
	// lock the processed_logs table
	if err := acquireLock(tx); err != nil {
		return nil, err
	}

	// fill temp table with batch logs
	if err := fillTempTable(tx, batch); err != nil {
		return nil, err
	}

	// find duplicated logs
	rows, err := tx.Query(`
		SELECT t.log_id
		FROM temp_logs t
		INNER JOIN processed_logs p ON t.log_id = p.log_id
	`)
	if err != nil {
		return nil, fmt.Errorf("error selecting new logs: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		var logId string
		if err := rows.Scan(&logId); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		// remove duplicated item from the batch
		delete(batch, logId)
	}

	err = fillLogs(tx, batch)
	if err != nil {
		return nil, err
	}

	return batch, nil
}

func acquireLock(tx *sql.Tx) error {
	_, err := tx.Exec("LOCK TABLE processed_logs IN ACCESS EXCLUSIVE MODE")
	if err != nil {
		return fmt.Errorf("error acquiring table lock: %s", err)
	}

	return nil
}
