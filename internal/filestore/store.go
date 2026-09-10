// Package filestore provides commit-oriented file destinations for the server.
package filestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	aliyunoss "github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go-sync/internal/serverconfig"
)

type Store interface {
	Put(context.Context, string, string, os.FileMode, time.Time) error
	Delete(context.Context, string) error
	Close() error
}

func Open(cfg serverconfig.FileStorage) (Store, error) {
	switch cfg.Backend {
	case "directory":
		if err := os.MkdirAll(cfg.Directory, 0o750); err != nil {
			return nil, err
		}
		root, err := os.OpenRoot(cfg.Directory)
		if err != nil {
			return nil, err
		}
		return &directoryStore{root: root, prefix: filepath.FromSlash(strings.Trim(cfg.Prefix, "/"))}, nil
	case "s3":
		endpoint, secure := cfg.Endpoint, cfg.Secure
		if strings.HasPrefix(endpoint, "https://") {
			endpoint, secure = strings.TrimPrefix(endpoint, "https://"), true
		} else if strings.HasPrefix(endpoint, "http://") {
			endpoint, secure = strings.TrimPrefix(endpoint, "http://"), false
		}
		client, err := minio.New(endpoint, &minio.Options{
			Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken), Secure: secure, Region: cfg.Region,
		})
		if err != nil {
			return nil, err
		}
		return &s3Store{client: client, bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
	case "oss":
		options := make([]aliyunoss.ClientOption, 0, 1)
		if cfg.SessionToken != "" {
			options = append(options, aliyunoss.SecurityToken(cfg.SessionToken))
		}
		client, err := aliyunoss.New(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, options...)
		if err != nil {
			return nil, err
		}
		bucket, err := client.Bucket(cfg.Bucket)
		if err != nil {
			return nil, err
		}
		return &ossStore{bucket: bucket, prefix: cfg.Prefix}, nil
	default:
		return nil, errors.New("file storage is not configured")
	}
}

func objectName(prefix, name string) (string, error) {
	if name == "" || !filepath.IsLocal(filepath.FromSlash(name)) || strings.ContainsRune(name, 0) {
		return "", errors.New("file path is not local")
	}
	return path.Join(strings.Trim(prefix, "/"), name), nil
}

type directoryStore struct {
	root   *os.Root
	prefix string
}

func (s *directoryStore) Put(ctx context.Context, name, source string, mode os.FileMode, modified time.Time) (result error) {
	rel, err := objectName(filepath.ToSlash(s.prefix), name)
	if err != nil {
		return err
	}
	rel = filepath.FromSlash(rel)
	if err := s.root.MkdirAll(filepath.Dir(rel), 0o750); err != nil {
		return err
	}
	tempDir := ".go-sync-tmp"
	if err := s.root.MkdirAll(tempDir, 0o700); err != nil {
		return err
	}
	tempName := filepath.Join(tempDir, uuid.NewString())
	destination, err := s.root.OpenFile(tempName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if err := s.root.Remove(tempName); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}()
	sourceFile, err := os.Open(source)
	if err != nil {
		return errors.Join(err, destination.Close())
	}
	defer sourceFile.Close()
	if _, err := copyContext(ctx, destination, sourceFile); err != nil {
		return errors.Join(err, destination.Close())
	}
	if err := destination.Sync(); err != nil {
		return errors.Join(err, destination.Close())
	}
	if err := destination.Close(); err != nil {
		return err
	}
	if mode.Perm() == 0 {
		mode = 0o640
	}
	if err := s.root.Chmod(tempName, mode.Perm()); err != nil {
		return err
	}
	if err := s.root.Chtimes(tempName, modified, modified); err != nil {
		return err
	}
	if err := s.root.Rename(tempName, rel); err == nil {
		return nil
	} else if runtime.GOOS != "windows" {
		return err
	}
	// Windows cannot replace an existing destination with Rename. The fallback
	// has a short visibility gap; first creation and Unix replacements are atomic.
	if err := s.root.Remove(rel); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.root.Rename(tempName, rel)
}

func (s *directoryStore) Delete(_ context.Context, name string) error {
	rel, err := objectName(filepath.ToSlash(s.prefix), name)
	if err != nil {
		return err
	}
	err = s.root.Remove(filepath.FromSlash(rel))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *directoryStore) Close() error { return s.root.Close() }

type s3Store struct {
	client *minio.Client
	bucket string
	prefix string
}

func (s *s3Store) Put(ctx context.Context, name, source string, _ os.FileMode, _ time.Time) error {
	key, err := objectName(s.prefix, name)
	if err != nil {
		return err
	}
	_, err = s.client.FPutObject(ctx, s.bucket, key, source, minio.PutObjectOptions{})
	return err
}

func (s *s3Store) Delete(ctx context.Context, name string) error {
	key, err := objectName(s.prefix, name)
	if err != nil {
		return err
	}
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

func (*s3Store) Close() error { return nil }

type ossStore struct {
	bucket *aliyunoss.Bucket
	prefix string
}

func (s *ossStore) Put(ctx context.Context, name, source string, _ os.FileMode, _ time.Time) error {
	key, err := objectName(s.prefix, name)
	if err != nil {
		return err
	}
	info, err := os.Stat(source)
	if err != nil {
		return err
	}
	if info.Size() <= 100<<20 {
		return s.bucket.PutObjectFromFile(key, source, aliyunoss.WithContext(ctx))
	}
	return s.bucket.UploadFile(key, source, 10<<20, aliyunoss.Routines(4), aliyunoss.WithContext(ctx))
}

func (s *ossStore) Delete(ctx context.Context, name string) error {
	key, err := objectName(s.prefix, name)
	if err != nil {
		return err
	}
	return s.bucket.DeleteObject(key, aliyunoss.WithContext(ctx))
}

func (*ossStore) Close() error { return nil }

func copyContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	var total int64
	buffer := make([]byte, 1<<20)
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, readErr := source.Read(buffer)
		if n > 0 {
			written, writeErr := destination.Write(buffer[:n])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != n {
				return total, io.ErrShortWrite
			}
		}
		if errors.Is(readErr, io.EOF) {
			return total, nil
		}
		if readErr != nil {
			return total, fmt.Errorf("read source: %w", readErr)
		}
	}
}
