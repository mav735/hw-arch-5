package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Exporter struct {
	s3     *minio.Client
	bucket string
	store  *Store
}

type exportFile struct {
	Date       string    `json:"date"`
	ExportedAt time.Time `json:"exported_at"`
	Metrics    []Metric  `json:"metrics"`
}

func NewExporter(endpoint, accessKey, secretKey, bucket string, store *Store) (*Exporter, error) {
	cl, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, "")})
	if err != nil {
		return nil, err
	}
	return &Exporter{s3: cl, bucket: bucket, store: store}, nil
}

func ObjectKey(day time.Time) string {
	return fmt.Sprintf("daily/%s/aggregates.json", day.Format(time.DateOnly))
}

// Export writes all metrics of the day to a fixed key, so a re-export overwrites it.
func (x *Exporter) Export(ctx context.Context, day time.Time) (string, int, error) {
	metrics, err := x.store.MetricsForDate(ctx, day)
	if err != nil {
		return "", 0, fmt.Errorf("read postgres: %w", err)
	}
	if len(metrics) == 0 {
		return "", 0, fmt.Errorf("no aggregates for %s", day.Format(time.DateOnly))
	}
	body, err := json.MarshalIndent(exportFile{Date: day.Format(time.DateOnly), ExportedAt: time.Now().UTC(), Metrics: metrics}, "", "  ")
	if err != nil {
		return "", 0, err
	}

	exists, err := x.s3.BucketExists(ctx, x.bucket)
	if err != nil {
		return "", 0, fmt.Errorf("s3: %w", err)
	}
	if !exists {
		if err := x.s3.MakeBucket(ctx, x.bucket, minio.MakeBucketOptions{}); err != nil {
			return "", 0, fmt.Errorf("s3 make bucket: %w", err)
		}
	}
	key := ObjectKey(day)
	_, err = x.s3.PutObject(ctx, x.bucket, key, bytes.NewReader(body), int64(len(body)),
		minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return "", 0, fmt.Errorf("s3 put: %w", err)
	}
	return key, len(metrics), nil
}
