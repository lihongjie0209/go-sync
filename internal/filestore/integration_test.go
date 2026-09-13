//go:build integration

package filestore

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go-sync/internal/serverconfig"
)

func TestS3StorePutReplaceAndDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	const accessKey, secretKey = "test-access", "test-secret-password"
	container, err := testcontainers.Run(ctx, "quay.io/minio/minio@sha256:d249d1fb6966de4d8ad26c04754b545205ff15a62e4fd19ebd0f26fa5baacbc0",
		testcontainers.WithEnv(map[string]string{"MINIO_ROOT_USER": accessKey, "MINIO_ROOT_PASSWORD": secretKey}),
		testcontainers.WithCmd("server", "/data"),
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/minio/health/ready").WithPort("9000/tcp").WithStartupTimeout(90*time.Second)),
	)
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatal(err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	store, err := Open(serverconfig.FileStorage{Backend: "s3", Endpoint: net.JoinHostPort(host, port.Port()), Bucket: "files", Prefix: "replica", AccessKey: accessKey, SecretKey: secretKey})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	s3 := store.(*s3Store)
	if err := s3.client.MakeBucket(ctx, s3.bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(source, []byte("object-body"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "nested/file.txt", source, 0, time.Time{}); err != nil {
		t.Fatal(err)
	}
	object, err := s3.client.GetObject(ctx, s3.bucket, "replica/nested/file.txt", minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(object)
	_ = object.Close()
	if err != nil || string(payload) != "object-body" {
		t.Fatalf("object payload = %q, error = %v", payload, err)
	}
	if err := store.Delete(ctx, "nested/file.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := s3.client.StatObject(ctx, s3.bucket, "replica/nested/file.txt", minio.StatObjectOptions{}); err == nil {
		t.Fatal("deleted object still exists")
	}
}
