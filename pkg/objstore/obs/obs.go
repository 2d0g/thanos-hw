package obs

import (
	"bufio"
	"context"
	"io"
        "fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	obs "github.com/2d0g/obs/obs"
	"github.com/go-kit/kit/log"
	"github.com/mozillazg/go-cos"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	yaml "gopkg.in/yaml.v2"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const dirDelim = "/"

// Bucket implements the store.Bucket interface against cos-compatible(Tencent Object Storage) APIs.
type Bucket struct {
	logger log.Logger
	client *obs.ObsClient
	name   string
}

// Config encapsulates the necessary config values to instantiate an cos client.
type Config struct {
	Bucket   string `yaml:"bucket"`
	Endpoint string `yaml:"endpoint"`
	AK       string `yaml:"AK"`
	SK       string `yaml:"SK"`
}

// Validate checks to see if mandatory cos config options are set.
func (conf *Config) validate() error {
	if conf.Bucket == "" ||
		conf.Endpoint == "" ||
		conf.AK == "" ||
		conf.SK == "" {
		return errors.New("insufficient obs configuration information")
	}
	return nil
}

func NewBucket(logger log.Logger, conf []byte, component string) (*Bucket, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	var config Config
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, errors.Wrap(err, "parsing obs configuration")
	}
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "validate obs configuration")
	}

	client, err := obs.New(config.AK, config.SK, config.Endpoint)
	if err != nil {
		panic(err)
	}

	bkt := &Bucket{
		logger: logger,
		client: client,
		name:   config.Bucket,
	}
	return bkt, nil
}

// Name returns the bucket name for COS.
func (b *Bucket) Name() string {
	return b.name
}

func (b *Bucket) DoConcurrentUploadPart(name string, filePath string) error {
	input := &obs.InitiateMultipartUploadInput{}
	input.Bucket = b.name
	input.Key = name
	output, err := b.client.InitiateMultipartUpload(input)
	if err != nil {
		return err
	}
	uploadId := output.UploadId

	var partSize int64 = 1024 * 1024 * 1024
	stat, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	partCount := int(fileSize / partSize)
	if fileSize%partSize != 0 {
		partCount++
	}

	partChan := make(chan obs.Part, 5)

	for i := 0; i < partCount; i++ {
		partNumber := i + 1
		offset := int64(i) * partSize
		currPartSize := partSize
		if i+1 == partCount {
			currPartSize = fileSize - offset
		}
		go func() {
			uploadPartInput := &obs.UploadPartInput{}
			uploadPartInput.Bucket = b.name
			uploadPartInput.Key = name
			uploadPartInput.UploadId = uploadId
			uploadPartInput.SourceFile = filePath
			uploadPartInput.PartNumber = partNumber
			uploadPartInput.Offset = offset
			uploadPartInput.PartSize = currPartSize
			uploadPartInputOutput, err := b.client.UploadPart(uploadPartInput)
			if err == nil {
				partChan <- obs.Part{ETag: uploadPartInputOutput.ETag, PartNumber: uploadPartInputOutput.PartNumber}
			} else {
				panic(err)
			}
		}()
	}

	parts := make([]obs.Part, 0, partCount)
	for {
		part, ok := <-partChan
		if !ok {
			break
		}
		parts = append(parts, part)
		if len(parts) == partCount {
			close(partChan)
		}
	}
	completeMultipartUploadInput := &obs.CompleteMultipartUploadInput{}
	completeMultipartUploadInput.Bucket = b.name
	completeMultipartUploadInput.Key = name
	completeMultipartUploadInput.UploadId = uploadId
	completeMultipartUploadInput.Parts = parts
	_, err = b.client.CompleteMultipartUpload(completeMultipartUploadInput)
	if err != nil {
		return err
	}
	return nil
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	tmpFilename := "/tmp/thanos.tmp"
	fo, err := os.Create(tmpFilename)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()

	w := bufio.NewWriter(fo)

	buf := make([]byte, 1024*1024*1024)
	for {
		// read a chunk
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}

		if _, err := w.Write(buf[:n]); err != nil {
			panic(err)
		}
	}

	if err = w.Flush(); err != nil {
		panic(err)
	}

	err = b.DoConcurrentUploadPart(name, tmpFilename)
	if err != nil {
		return errors.Wrap(err, "upload oss object")
	}

	err = os.Remove(tmpFilename)
	if err != nil {
		return errors.Wrap(err, "deleted tmp uploadFile")
	}

	return nil

}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	input := &obs.DeleteObjectInput{}
	input.Bucket = b.name
	input.Key = name
	ok, _ := b.Exists(ctx, name)
	if !ok {
		return errors.Wrap(errors.New("delete obs object"), "delete obs object")
	}
	_, err := b.client.DeleteObject(input)
	if err != nil {
		return errors.Wrap(err, "delete obs object")
	}
	return nil
}

func (b *Bucket) listObjects(dir string) ([]string, error) {
	input := &obs.ListObjectsInput{}
	input.Bucket = b.name
	input.MaxKeys = 1000
	input.Prefix = dir
	input.Delimiter = "/"
	//  input.Prefix = "src/"
	var listNames []string
	for {
		output, err := b.client.ListObjects(input)
		if err == nil {
			for _, val := range output.Contents {
				listNames = append(listNames, val.Key)
			}
			for _, val := range output.CommonPrefixes {
				listNames = append(listNames, val)
			}
			if output.IsTruncated {
				input.Marker = output.NextMarker
			} else {
				break
			}
		} else {
			if obsError, ok := err.(obs.ObsError); ok {
				fmt.Printf("Code:%s\n", obsError.Code)
				fmt.Printf("Message:%s\n", obsError.Message)
			}
			break
		}
	}
	return listNames, nil
}

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	if dir != "" {
		dir = strings.TrimSuffix(dir, dirDelim) + dirDelim
	}

	names, err := b.listObjects(dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := f(name); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bucket) getRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if len(name) == 0 {
		return nil, errors.Errorf("given object name should not empty")
	}

	//opts := &cos.ObjectGetOptions{}

	input := &obs.GetObjectInput{}
	input.Bucket = b.name
	input.Key = name

	if length != -1 {
		input.RangeStart = off
		input.RangeEnd = off + length - 1
	}

	resp, err := b.client.GetObject(input)

	if err != nil {
		return nil, err
	}

	if _, err := resp.Body.Read(nil); err != nil {
		runutil.ExhaustCloseWithLogOnErr(b.logger, resp.Body, "obs get range obj close")
		return nil, err
	}

	return resp.Body, nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getRange(ctx, name, 0, -1)
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getRange(ctx, name, off, length)
}

// Exists checks if the given object exists in the bucket.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	input := &obs.GetObjectMetadataInput{}
	input.Bucket = b.name
	input.Key = name
	_, err := b.client.GetObjectMetadata(input)
	if err != nil {
		if _, ok := err.(obs.ObsError); ok {
			return false, nil
		} else {
			return false, errors.Wrap(err, "head cos object")
		}
	}
	return true, nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	return strings.Contains(err.Error(), "404")
}

func (b *Bucket) Close() error { return nil }

type objectInfo struct {
	key string
	err error
}

//func (b *Bucket) listObjects(ctx context.Context, objectPrefix string) <-chan objectInfo {
//	objectsCh := make(chan objectInfo, 1)
//
//	go func(objectsCh chan<- objectInfo) {
//		defer close(objectsCh)
//		var marker string
//		for {
//			result, _, err := b.client.Bucket.Get(ctx, &cos.BucketGetOptions{
//				Prefix:    objectPrefix,
//				MaxKeys:   1000,
//				Marker:    marker,
//				Delimiter: dirDelim,
//			})
//			if err != nil {
//				select {
//				case objectsCh <- objectInfo{
//					err: err,
//				}:
//				case <-ctx.Done():
//				}
//				return
//			}
//
//			for _, object := range result.Contents {
//				select {
//				case objectsCh <- objectInfo{
//					key: object.Key,
//				}:
//				case <-ctx.Done():
//					return
//				}
//			}
//
//			// The result of CommonPrefixes contains the objects
//			// that have the same keys between Prefix and the key specified by delimiter.
//			for _, obj := range result.CommonPrefixes {
//				select {
//				case objectsCh <- objectInfo{
//					key: obj,
//				}:
//				case <-ctx.Done():
//					return
//				}
//			}
//
//			if !result.IsTruncated {
//				return
//			}
//
//			marker = result.NextMarker
//		}
//	}(objectsCh)
//	return objectsCh
//}

func setRange(opts *cos.ObjectGetOptions, start, end int64) error {
	if start == 0 && end < 0 {
		opts.Range = fmt.Sprintf("bytes=%d", end)
	} else if 0 < start && end == 0 {
		opts.Range = fmt.Sprintf("bytes=%d-", start)
	} else if 0 <= start && start <= end {
		opts.Range = fmt.Sprintf("bytes=%d-%d", start, end)
	} else {
		return errors.Errorf("Invalid range specified: start=%d end=%d", start, end)
	}
	return nil
}

func configFromEnv() Config {
	c := Config{
		Bucket:   os.Getenv("OBS_BUCKET"),
		Endpoint: os.Getenv("OBS_ENDPOINT"),
		AK:       os.Getenv("OBS_AK"),
		SK:       os.Getenv("OBS_SK"),
	}
	return c
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB) (objstore.Bucket, func(), error) {
	c := configFromEnv()
	if err := validateForTest(c); err != nil {
		return nil, nil, err
	}

	if c.Bucket != "" {
		if os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
			return nil, nil, errors.New("COS_BUCKET is defined. Normally this tests will create temporary bucket " +
				"and delete it after test. Unset COS_BUCKET env variable to use default logic. If you really want to run " +
				"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
				"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
				"to safety (accidentally pointing prod bucket for test) as well as COS not being fully strong consistent.")
		}

		bc, err := yaml.Marshal(c)
		if err != nil {
			return nil, nil, err
		}

		b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-test")
		if err != nil {
			return nil, nil, err
		}

		if err := b.Iter(context.Background(), "", func(f string) error {
			return errors.Errorf("bucket %s is not empty", c.Bucket)
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "cos check bucket %s", c.Bucket)
		}

		t.Log("WARNING. Reusing", c.Bucket, "COS bucket for COS tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}

	src := rand.NewSource(time.Now().UnixNano())

	tmpBucketName := strings.Replace(fmt.Sprintf("test_%x", src.Int63()), "_", "-", -1)
	if len(tmpBucketName) >= 31 {
		tmpBucketName = tmpBucketName[:31]
	}
	c.Bucket = tmpBucketName

	bc, err := yaml.Marshal(c)
	if err != nil {
		return nil, nil, err
	}

	b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-test")
	if err != nil {
		return nil, nil, err
	}

	//if _, err := b.client.PutObject(context.Background(), nil); err != nil {
	//	return nil, nil, err
	//}
	t.Log("created temporary OBS bucket for OBS tests with name", tmpBucketName)

	return b, func() {
		//objstore.EmptyBucket(t, context.Background(), b)
		//if _, err := b.client.Bucket.Delete(context.Background()); err != nil {
		//	t.Logf("deleting bucket %s failed: %s", tmpBucketName, err)
		//}
	}, nil
}

func validateForTest(conf Config) error {
	if conf.Bucket == "" ||
		conf.Endpoint == "" ||
		conf.AK == "" ||
		conf.SK == "" {
		return errors.New("insufficient obs configuration information")
	}
	return nil
}
