package db

import (
	"context"
	"time"

	"errors"
	"github.com/sa7mon/s3scanner/bucket"
	log "github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var db *gorm.DB

type Logger struct{}

func (l *Logger) LogMode(logger.LogLevel) logger.Interface {
	return l
}

func (l *Logger) Info(ctx context.Context, s string, args ...any) {
	log.WithContext(ctx).Infof(s, args...)
}

func (l *Logger) Warn(ctx context.Context, s string, args ...any) {
	log.WithContext(ctx).Warnf(s, args...)
}

func (l *Logger) Error(ctx context.Context, s string, args ...any) {
	log.WithContext(ctx).Errorf(s, args...)
}

func (l *Logger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	ignoredError := gorm.ErrRecordNotFound
	slowThreshold := time.Hour
	elapsed := time.Since(begin)
	sql, _ := fc()
	fields := log.Fields{}
	// ignore RecordNotFound error
	if err != nil && !errors.Is(err, ignoredError) {
		fields[log.ErrorKey] = err
		log.WithContext(ctx).WithFields(fields).Errorf("%s [%s]", sql, elapsed)
	}

	if elapsed > slowThreshold {
		log.WithContext(ctx).WithFields(fields).Warnf("%s [%s]", sql, elapsed)
	}
}

func Connect(dbConn string, migrate bool) error {
	// Connect to the database and run migrations if needed

	// We've already connected
	// TODO: Replace this with a sync.Once pattern
	if db != nil {
		return nil
	}

	// https://github.com/go-gorm/postgres
	database, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  dbConn,
		PreferSimpleProtocol: true, // disables implicit prepared statement usage
	}), &gorm.Config{
		Logger: new(Logger),
	})

	if err != nil {
		return err
	}

	if migrate {
		if err := database.AutoMigrate(&bucket.Bucket{}, &bucket.BucketObject{}); err != nil {
			return err
		}
	}

	db = database

	return nil
}
func StoreBucket(b *bucket.Bucket) error {
	if b.Exists == bucket.BucketNotExist {
		return nil
	}
	return db.Session(&gorm.Session{CreateBatchSize: 1000, FullSaveAssociations: true}).Create(&b).Error
}
