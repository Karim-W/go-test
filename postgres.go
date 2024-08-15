package go_test

import (
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/karim-w/stdlib/sqldb"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
	"go.uber.org/zap"
)

func createPostgresDb() (db *sql.DB, cleanup CleanupFunc, err error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Printf("Could not construct pool: %s\n", err)
		return
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Printf("Could not connect to Docker: %s\n", err)
		return
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "16",
		Env: []string{
			"POSTGRES_PASSWORD=secret",
			"POSTGRES_USER=postgres",
			"POSTGRES_DB=test",
			"listen_addresses = '*'",
		},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Printf("Could not start resource: %v\n", err)
		return
	}

	hostAndPort := resource.GetHostPort("5432/tcp")
	databaseUrl := fmt.Sprintf("postgres://postgres:secret@%s/test?sslmode=disable", hostAndPort)

	log.Println("Connecting to database on url: ", databaseUrl)

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 120 * time.Second
	if err = pool.Retry(func() error {
		db, err = sql.Open("postgres", databaseUrl)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Printf("Could not connect to docker: %s\n", err)
		return
	}

	cleanup = func() {
		if err := pool.Purge(resource); err != nil {
			log.Printf("Could not purge resource: %s\n", err)
			return
		}
	}

	return db, cleanup, nil
}

func InitDockerPostgres() (*sql.DB, CleanupFunc, error) {
	return createPostgresDb()
}

func InitDockerPostgresTest(t *testing.T) (*sql.DB, CleanupFunc) {
	db, cleanup, err := createPostgresDb()
	if err != nil {
		t.Fatalf("Could not create postgres db: %s", err)
	}
	return db, cleanup
}

func InitDockerPostgresSQLDBTest(t *testing.T) (sqldb.DB, CleanupFunc) {
	db, cleanup := InitDockerPostgresTest(t)

	return sqldb.DBWarpper(db, nil, "test", zap.NewExample()), cleanup
}

func PostgresConnectionString() (dsn string, cleanup CleanupFunc, err error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Printf("Could not construct pool: %s\n", err)
		return
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Printf("Could not connect to Docker: %s\n", err)
		return
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "16",
		Env: []string{
			"POSTGRES_PASSWORD=secret",
			"POSTGRES_USER=postgres",
			"POSTGRES_DB=test",
			"listen_addresses = '*'",
		},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Printf("Could not start resource: %v\n", err)
		return
	}

	hostAndPort := resource.GetHostPort("5432/tcp")
	databaseUrl := fmt.Sprintf("postgres://postgres:secret@%s/test?sslmode=disable", hostAndPort)

	log.Println("Connecting to database on url: ", databaseUrl)

	pool.MaxWait = 120 * time.Second

	if err = pool.Retry(func() error {
		db, err := sql.Open("postgres", databaseUrl)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Printf("Could not connect to docker: %s\n", err)
		return
	}

	cleanup = func() {
		if err := pool.Purge(resource); err != nil {
			log.Printf("Could not purge resource: %s\n", err)
			return
		}
	}

	return databaseUrl, cleanup, nil
}

func PostgresConnectionStringTest(t *testing.T) (dsn string, cleanup CleanupFunc) {
	dsn, cleanup, err := PostgresConnectionString()
	if err != nil {
		t.Fatalf("Could not create postgres db: %s", err)
	}
	return dsn, cleanup
}
