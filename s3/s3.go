package ceph_go

import (
	"context"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	s3Client "git.wildberries.ru/infrastructure/ceph-go/pkg/apiservice/httpclient"
)

//CephGO ...
type CephGO struct {
	cli s3Client.Client

	bucket string

	logger log.Logger
}

//DownloadFile ...
func (d *CephGO) DownloadFile(ctx context.Context, fileName, keyS3File string) (err error) {
	var (
		fd *os.File
	)

	if fd, err = os.Create(fileName); err != nil {
		return err
	}

	defer func() {
		err = fd.Close()
		if err != nil {
			_ = level.Error(d.logger).Log("msg", "failed to close file ", "err", err)
		}
	}()

	ctxDownloadWithContext, f := context.WithTimeout(ctx, time.Minute*5)
	defer f()

	requestDownload := &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(keyS3File),
	}
	_, err = d.cli.DownloadWithContext(ctxDownloadWithContext, fd, requestDownload)
	if err != nil {
		return
	}

	return
}

//UploadFile ...
func (d *CephGO) UploadFile(ctx context.Context, fileName, keyS3File string) (err error) {

	fd, err := os.Open(fileName)
	if err != nil {
		return err
	}

	defer func() {
		err = fd.Close()
		if err != nil {
			_ = level.Error(d.logger).Log("msg", "failed to close file ", "err", err)
		}
	}()

	ctxUploadWithContext, f := context.WithTimeout(ctx, time.Minute * 5)
	defer f()

	requestUpload := &s3manager.UploadInput{
		Bucket : aws.String(d.bucket),
		Key : aws.String(keyS3File),
		Body: fd,
	}
	_, err = d.cli.UploadWithContext(ctxUploadWithContext, requestUpload)
	if err != nil {
		return
	}

	return
}

//NewDatabase ...
func NewDatabase(cli s3Client.Client, bucket string, logger log.Logger) (dat *CephGO) {
	return &CephGO{
		cli:    cli,
		bucket: bucket,
		logger: logger,
	}
}
