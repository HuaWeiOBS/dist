// Package obs provides a storagedriver.StorageDriver implementation to
// store blobs in Huawei Cloud OBS cloud storage.
//
// This package leverages the official Huawei cloud client library for interfacing with
// obs.
//
// Because OBS is a key, value store the Stat call does not support last modification
// time for directories (directories are an abstraction for key, value stores)
//
// +build include_obs

package obs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"eSDK_Storage_OBS_V2.2.1_Go/src/obs"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "obs"

const (
	// minChunkSize defines the minimum multipart upload chunk size
	// OBS API requires multipart upload chunks to be at least 5MB
	minChunkSize = 5 << 20

	// maxChunkSize defines the maximum multipart upload chunk size allowed by OBS. (5GB)
	maxChunkSize = 5 << 30

	defaultChunkSize = 40*minChunkSize

	// defaultMultipartCopyChunkSize defines the default chunk size for all
	// but the last Upload Part - Copy operation of a multipart copy.
	// Empirically, 32 MB is optimal.
	defaultMultipartCopyChunkSize = 32 << 20

	// defaultMultipartCopyMaxConcurrency defines the default maximum number
	// of concurrent Upload Part - Copy operations for a multipart copy.
	defaultMultipartCopyMaxConcurrency = 100

	// defaultMultipartCopyThresholdSize defines the default object size
	// above which multipart copy will be used. (PUT Object - Copy is used
	// for objects at or below this size.)  Empirically, 32 MB is optimal.
	defaultMultipartCopyThresholdSize = 32 << 20
)

// listMax is the largest amount of objects you can request from OBS in a list call
const listMax = 1000

// noStorageClass defines the value to be used if storage class is not supported by the OBS endpoint
const noStorageClass = obs.StorageClassType("")

// validRegions maps known obs region identifiers to region descriptors
var validRegions = map[string]struct{}{}

// validObjectACLs contains known obs object Acls
var validObjectACLs = map[obs.AclType]struct{}{}

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	AccessKey                   string
	SecretKey                   string
	Bucket                      string
	Region                      string
	Endpoint                    string
	Secure                      bool
	V2Auth                      obs.SignatureType
	ChunkSize                   int64
	MultipartCopyChunkSize      int64
	MultipartCopyMaxConcurrency int64
	MultipartCopyThresholdSize  int64
	RootDirectory               string
	StorageClass                obs.StorageClassType
	ObjectACL                   obs.AclType
	//Encrypt                     bool
	//KeyID                       string
	//SecurityToken               string
	PathStyle bool
}

func init() {
	for _, region := range []string{
		"cn-north-1",
		"cn-east-2",
		"cn-south-1",
		"ap-southeast-1",
	} {
		validRegions[region] = struct{}{}
	}

	for _, objectACL := range []obs.AclType{
		obs.AclPrivate,
		obs.AclPublicRead,
		obs.AclPublicReadWrite,
		obs.AclAuthenticatedRead,
		obs.AclBucketOwnerRead,
		obs.AclBucketOwnerFullControl,
		obs.AclLogDeliveryWrite,
	} {
		validObjectACLs[objectACL] = struct{}{}
	}

	factory.Register(driverName, &obsDriverFactory{})
}

// obsDriverFactory implements the factory.StorageDriverFactory interface
type obsDriverFactory struct{}

func (factory *obsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	Client                      *obs.ObsClient
	Bucket                      string
	ChunkSize                   int64
	MultipartCopyChunkSize      int64
	MultipartCopyMaxConcurrency int64
	MultipartCopyThresholdSize  int64
	RootDirectory               string
	StorageClass                obs.StorageClassType
	ObjectACL                   obs.AclType
	//Encrypt                     bool
	//KeyID                       string
	PathStyle bool
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Huawei Cloud OBS
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - accesskey
// - secretkey
// - region
// - endPoint
// - bucketname
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	accessKey := parameters["accesskey"]
	if accessKey == nil {
		accessKey = ""
	}
	secretKey := parameters["secretkey"]
	if secretKey == nil {
		secretKey = ""
	}

	bucket := parameters["bucket"]
	if bucket == nil || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

	endpoint := parameters["endpoint"]
	if endpoint == nil || fmt.Sprint(endpoint) == "" {
		return nil, fmt.Errorf("No endpoint parameter provided")
	}

	region := ""
	regionName := parameters["region"]
	if regionName == nil || fmt.Sprint(regionName) == "" {
		return nil, fmt.Errorf("No region parameter provided")
	} else {
		regionString, ok := regionName.(string)
		if !ok {
			return nil, fmt.Errorf("Invalid value for region parameter: %v", regionName)
		}

		if _, ok = validRegions[regionString]; !ok {
			return nil, fmt.Errorf("Invalid value for region parameter: %v", regionName)
		}
		region = regionString
	}

	secureBool := false
	secure, ok := parameters["secure"]
	switch secure := secure.(type) {
	case string:
		b, err := strconv.ParseBool(secure)
		if err != nil {
			return nil, fmt.Errorf("The secure parameter should be a boolean")
		}
		secureBool = b
	case bool:
		secureBool = secure
	case nil:
		// do nothing
	default:
		return nil, fmt.Errorf("The secure parameter should be a boolean")
	}

	v2 := obs.SignatureV2
	v2auth := parameters["v2auth"]
	switch v2auth := v2auth.(type) {
	case string:
		v2 = obs.SignatureType(v2auth)
		if !(v2 == obs.SignatureV2 || v2 == obs.SignatureV4) {
			return nil, fmt.Errorf("The v2auth parameter should be a SignatureType")
		}
	case obs.SignatureType:
		v2 = v2auth
	case nil:
		// do nothing
	default:
		return nil, fmt.Errorf("The v2auth parameter should be a SignatureType")
	}

	chunkSize, err := getParameterAsInt64(parameters, "chunksize", defaultChunkSize, minChunkSize, maxChunkSize)
	if err != nil {
		return nil, err
	}

	multipartCopyChunkSize, err := getParameterAsInt64(parameters, "multipartcopychunksize", defaultMultipartCopyChunkSize, minChunkSize, maxChunkSize)
	if err != nil {
		return nil, err
	}

	multipartCopyMaxConcurrency, err := getParameterAsInt64(parameters, "multipartcopymaxconcurrency", defaultMultipartCopyMaxConcurrency, 1, math.MaxInt64)
	if err != nil {
		return nil, err
	}

	multipartCopyThresholdSize, err := getParameterAsInt64(parameters, "multipartcopythresholdsize", defaultMultipartCopyThresholdSize, 0, maxChunkSize)
	if err != nil {
		return nil, err
	}

	rootDirectory, ok := parameters["rootdirectory"]
	if !ok {
		rootDirectory = ""
	}

	storageClass := obs.StorageClassStandard
	storageClassParam := parameters["storageclass"]
	if storageClassParam != nil {
		storageClassValue, ok := storageClassParam.(obs.StorageClassType)
		if !ok {
			return nil, fmt.Errorf("The storageclass parameter must be one of %v, %v invalid",
				[]string{string(obs.StorageClassStandard), string(obs.StorageClassWarm)}, storageClassParam)
		}
		// All valid storage class parameters are UPPERCASE, so be a bit more flexible here
		storageClassValue = storageClassValue
		if storageClassValue != noStorageClass &&
			storageClassValue != obs.StorageClassStandard &&
			storageClassValue != obs.StorageClassWarm {
			return nil, fmt.Errorf("The storageclass parameter must be one of %v, %v invalid",
				[]string{string(noStorageClass), string(obs.StorageClassStandard), string(obs.StorageClassWarm)}, storageClassParam)
		}
		storageClass = obs.StorageClassType(storageClassValue)
	}

	objectACL := obs.AclPrivate
	objectACLParam := parameters["objectacl"]
	if objectACLParam != nil {
		objectACLValue, ok := objectACLParam.(obs.AclType)
		if !ok {
			return nil, fmt.Errorf("Invalid value for objectacl parameter: %v", objectACLParam)
		}
		if _, ok = validObjectACLs[objectACLValue]; !ok {
			return nil, fmt.Errorf("Invalid value for objectacl parameter: %v", objectACLParam)
		}
		objectACL = objectACLValue
	}

	//encryptBool := false
	//encrypt := parameters["encrypt"]
	//switch encrypt := encrypt.(type) {
	//case string:
	//	b, err := strconv.ParseBool(encrypt)
	//	if err != nil {
	//		return nil, fmt.Errorf("The encrypt parameter should be a boolean")
	//	}
	//	encryptBool = b
	//case bool:
	//	encryptBool = encrypt
	//case nil:
	//	// do nothing
	//default:
	//	return nil, fmt.Errorf("The encrypt parameter should be a boolean")
	//}
	//
	//keyID := parameters["keyid"]
	//if keyID == nil {
	//	keyID = ""
	//}
	//
	//securityToken := ""

	pathStyleBool := false
	pathStyle := parameters["pathstyle"]
	switch pathStyle := pathStyle.(type) {
	case string:
		b, err := strconv.ParseBool(pathStyle)
		if err != nil {
			return nil, fmt.Errorf("The pathStyle parameter should be a boolean")
		}
		pathStyleBool = b
	case bool:
		pathStyleBool = pathStyle
	case nil:
		// do nothing
	default:
		return nil, fmt.Errorf("The pathStyle parameter should be a boolean")
	}

	params := DriverParameters{
		fmt.Sprint(accessKey),
		fmt.Sprint(secretKey),
		fmt.Sprint(bucket),
		region,
		fmt.Sprint(endpoint),
		secureBool,
		v2,
		chunkSize,
		multipartCopyChunkSize,
		multipartCopyMaxConcurrency,
		multipartCopyThresholdSize,
		fmt.Sprint(rootDirectory),
		storageClass,
		objectACL,
		//encryptBool,
		//fmt.Sprint(keyID),
		//fmt.Sprint(securityToken),
		pathStyleBool,
	}

	return New(params)
}

// getParameterAsInt64 converts paramaters[name] to an int64 value (using
// defaultt if nil), verifies it is no smaller than min, and returns it.
func getParameterAsInt64(parameters map[string]interface{}, name string, defaultt int64, min int64, max int64) (int64, error) {
	rv := defaultt
	param := parameters[name]
	switch v := param.(type) {
	case string:
		vv, err := strconv.ParseInt(v, 0, 64)
		if err != nil {
			return 0, fmt.Errorf("%s parameter must be an integer, %v invalid", name, param)
		}
		rv = vv
	case int64:
		rv = v
	case int, uint, int32, uint32, uint64:
		rv = reflect.ValueOf(v).Convert(reflect.TypeOf(rv)).Int()
	case nil:
		// do nothing
	default:
		return 0, fmt.Errorf("invalid value for %s: %#v", name, param)
	}

	if rv < min || rv > max {
		return 0, fmt.Errorf("The %s %#v parameter should be a number between %d and %d (inclusive)", name, rv, min, max)
	}
	return rv, nil
}

// New constructs a new Driver with the given
// Huawei Cloud credentials, region and bucketName
func New(params DriverParameters) (*Driver, error) {
        defer obs.CloseLog()
        //obs.SyncLog() 
	client, _ := obs.New(
		params.AccessKey,
		params.SecretKey,
		params.Endpoint,
		obs.WithRegion(params.Region),
		obs.WithSslVerify(params.Secure),
		obs.WithSignature(params.V2Auth),
		obs.WithPathStyle(params.PathStyle),
		//obs.WithSecurityToken(params.SecurityToken),
	)

	// Create a bucket with given name
	_, err := client.CreateBucket(&obs.CreateBucketInput{
		Bucket:       params.Bucket,
		ACL:          params.ObjectACL,
		StorageClass: params.StorageClass,
		BucketLocation: obs.BucketLocation{
			Location: params.Region,
		},
	})
	if err != nil {
		return nil, err
	}

	d := &driver{
		Client:                      client,
		Bucket:                      params.Bucket,
		ChunkSize:                   params.ChunkSize,
		MultipartCopyChunkSize:      params.MultipartCopyChunkSize,
		MultipartCopyMaxConcurrency: params.MultipartCopyMaxConcurrency,
		MultipartCopyThresholdSize:  params.MultipartCopyThresholdSize,
		RootDirectory:               params.RootDirectory,
		StorageClass:                params.StorageClass,
		ObjectACL:                   params.ObjectACL,
		PathStyle:                   params.PathStyle,
	}
	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface
func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
        reader, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(reader)
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	_, err := d.Client.PutObject(&obs.PutObjectInput{
		PutObjectBasicInput: obs.PutObjectBasicInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket:       d.Bucket,
				Key:          d.obsPath(path),
				ACL:          d.getACL(),
				StorageClass: d.getStorageClass(),
				//SseHeader:    obs.SseKmsHeader{Encryption: obs.DEFAULT_SSE_KMS_ENCRYPTION},
			},
			ContentType: d.getContentType(),
		},
		Body: bytes.NewReader(contents),
	})
	return parseError(path, err)
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	output, err := d.Client.GetObject(&obs.GetObjectInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: d.Bucket,
			Key:    d.obsPath(path),
		},
		RangeStart: offset,
	})

	if err != nil {
		if obsErr, ok := err.(obs.ObsError); ok && obsErr.Code == "InvalidRange" {
			return ioutil.NopCloser(bytes.NewReader(nil)), nil
		}

		return nil, parseError(path, err)
	}
	return output.Body, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	key := d.obsPath(path)
	if !append {
		// TODO (brianbland): cancel other uploads at this path
	//	obs.InitLog("/obs_log/OBS-SDK.log", 1024*1024*100, 10, obs.LEVEL_INFO, false)
	//	obs.SyncLog()
		output, err := d.Client.InitiateMultipartUpload(&obs.InitiateMultipartUploadInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket:       d.Bucket,
				Key:          key,
				ACL:          d.getACL(),
				StorageClass: d.getStorageClass(),
				//SseHeader:    obs.SseKmsHeader{Encryption: obs.DEFAULT_SSE_KMS_ENCRYPTION},
			},
			ContentType: d.getContentType(),
		})
		if err != nil {
			return nil, err
		}
		return d.newWriter(key, output.UploadId, nil), nil
	}

	output, err := d.Client.ListMultipartUploads(&obs.ListMultipartUploadsInput{
		Bucket: d.Bucket,
		Prefix: key,
	})
	if err != nil {
		return nil, parseError(path, err)
	}

	for _, multi := range output.Uploads {
		if key != multi.Key {
			continue
		}
		output, err := d.Client.ListParts(&obs.ListPartsInput{
			Bucket:   d.Bucket,
			Key:      key,
			UploadId: multi.UploadId,
		})
		if err != nil {
			return nil, parseError(path, err)
		}

		var multiSize int64
		for _, part := range output.Parts {
			multiSize += part.Size
		}
		return d.newWriter(key, multi.UploadId, output.Parts), nil
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	output, err := d.Client.ListObjects(&obs.ListObjectsInput{
		ListObjsInput: obs.ListObjsInput{
			Prefix:  d.obsPath(path),
			MaxKeys: 1,
		},
		Bucket: d.Bucket,
	})
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(output.Contents) == 1 {
		if output.Contents[0].Key != d.obsPath(path) {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			fi.Size = output.Contents[0].Size
			fi.ModTime = output.Contents[0].LastModified
		}
	} else if len(output.CommonPrefixes) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	path := opath
	if path != "/" && opath[len(path)-1] != '/' {
		path = path + "/"
	}

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp
	prefix := ""
	if d.obsPath("") == "" {
		prefix = "/"
	}

	output, err := d.Client.ListObjects(&obs.ListObjectsInput{
		ListObjsInput: obs.ListObjsInput{
			Prefix:    d.obsPath(path),
			MaxKeys:   listMax,
			Delimiter: "/",
		},
		Bucket: d.Bucket,
	})
	if err != nil {
		return nil, parseError(opath, err)
	}

	files := []string{}
	directories := []string{}

	for {
		for _, key := range output.Contents {
			files = append(files, strings.Replace(key.Key, d.obsPath(""), prefix, 1))
		}

		for _, commonPrefix := range output.CommonPrefixes {
//                        commonPrefix := commonPrefix>Prefix
			directories = append(directories, strings.Replace(commonPrefix[0:len(commonPrefix)-1], d.obsPath(""), prefix, 1))
		}

		if output.IsTruncated {
			output, err = d.Client.ListObjects(&obs.ListObjectsInput{
				ListObjsInput: obs.ListObjsInput{
					Prefix:    d.obsPath(path),
					Delimiter: "/",
					MaxKeys:   listMax,
				},
				Bucket: d.Bucket,
				Marker: output.NextMarker,
			})
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}

	if opath != "/" {
		if len(files) == 0 && len(directories) == 0 {
			// Treat empty output as missing directory, since we don't actually
			// have directories in obs.
			return nil, storagedriver.PathNotFoundError{Path: opath}
		}
	}
	return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	if err := d.copy(ctx, sourcePath, destPath); err != nil {
		return err
	}
	return d.Delete(ctx, sourcePath)
}

// copy copies an object stored at sourcePath to destPath.
func (d *driver) copy(ctx context.Context, sourcePath string, destPath string) error {
	// OBS can copy objects up to 5 GB in size with a single PUT Object - Copy
	// operation. For larger objects, the multipart upload API must be used.
	//
	// For each part except the last part, file size has to be in the range of
	//[100KB, 5GB], the last part should be at least 100KB.
        fileInfo, err := d.Stat(ctx, sourcePath)
	if err != nil {
		return parseError(sourcePath, err)
	}
	if fileInfo.Size() <= d.MultipartCopyThresholdSize {
		_, err := d.Client.CopyObject(&obs.CopyObjectInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket:       d.Bucket,
				Key:          d.obsPath(destPath),
				ACL:          d.getACL(),
				StorageClass: d.getStorageClass(),
				//SseHeader:    obs.SseKmsHeader{Encryption: obs.DEFAULT_SSE_KMS_ENCRYPTION},
			},
			CopySourceBucket: d.Bucket,
			CopySourceKey:    d.obsPath(sourcePath),
			ContentType:      d.getContentType(),
		})

		if err != nil {
			return parseError(sourcePath, err)
		}
		return nil
	}
	initOutput, err := d.Client.InitiateMultipartUpload(&obs.InitiateMultipartUploadInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket:       d.Bucket,
			Key:          d.obsPath(destPath),
			ACL:          d.getACL(),
			StorageClass: d.getStorageClass(),
			//SseHeader:    obs.SseKmsHeader{Encryption: obs.DEFAULT_SSE_KMS_ENCRYPTION},
		},
		ContentType: d.getContentType(),
	})
	if err != nil {
		return err
	}
	numParts := (fileInfo.Size() + d.MultipartCopyChunkSize - 1) / d.MultipartCopyChunkSize
	completedParts := make([]obs.Part, numParts)
	errChan := make(chan error, numParts)
	limiter := make(chan struct{}, d.MultipartCopyMaxConcurrency)
	for i := range completedParts {
	        i := int64(i)
		go func() {
			limiter <- struct{}{}
			firstByte := i * d.MultipartCopyChunkSize
			lastByte := firstByte + d.MultipartCopyChunkSize - 1
			if lastByte >= fileInfo.Size() {
				lastByte = fileInfo.Size() - 1
			}

			uploadOutput, err := d.Client.CopyPart(&obs.CopyPartInput{
				Bucket:     d.Bucket,
				Key:        d.obsPath(destPath),
                                UploadId:   initOutput.UploadId,
                                PartNumber: int(i + 1),
                                CopySourceBucket: d.Bucket,
				CopySourceKey: d.obsPath(sourcePath),
				CopySourceRangeStart: firstByte,
                                CopySourceRangeEnd: lastByte,
			})
			if err == nil {
				completedParts[i] = obs.Part{
					ETag:       uploadOutput.ETag,
					PartNumber: int(i + 1),
				}
			}
			errChan <- err
			<-limiter
		}()
	}
	for range completedParts {
		err := <-errChan
		if err != nil {
			return err
		}
	}
	_, err = d.Client.CompleteMultipartUpload(&obs.CompleteMultipartUploadInput{
		Bucket:   d.Bucket,
		Key:      d.obsPath(destPath),
		UploadId: initOutput.UploadId,
		Parts:    completedParts,
	})
	return err
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	obsObjects := make([]obs.ObjectToDelete, 0, listMax)
	obsPath := d.obsPath(path)
	listObjectsInput := &obs.ListObjectsInput{
		ListObjsInput: obs.ListObjsInput{
			Prefix: obsPath,
		},
		Bucket: d.Bucket,
	}

ListLoop:
	for {
		// list all the objects
		output, err := d.Client.ListObjects(listObjectsInput)

		// output.Contents can only be empty on the first call
		// if there were no more results to return after the first call, output.IsTruncated would have been false
		// and the loop would be exited without recalling ListObjects
		if err != nil || len(output.Contents) == 0 {
			return storagedriver.PathNotFoundError{Path: path}
		}

		for _, key := range output.Contents {
			// Stop if we encounter a key that is not a subpath (so that deleting "/a" does not delete "/ab").
			if len(key.Key) > len(obsPath) && (key.Key)[len(obsPath)] != '/' {
				break ListLoop
			}
			obsObjects = append(obsObjects, obs.ObjectToDelete{
				Key: key.Key,
			})
		}

		// output.Contents must have at least one element or we would have returned not found
		listObjectsInput.Marker = output.Contents[len(output.Contents)-1].Key

		// from the obs api docs, IsTruncated "specifies whether (true) or not (false) all of the results were returned"
		// if everything has been returned, break
		if !output.IsTruncated {
			break
		}
	}

	// need to chunk objects into groups of 1000 per obs restrictions
	total := len(obsObjects)
	for i := 0; i < total; i += 1000 {
		_, err := d.Client.DeleteObjects(&obs.DeleteObjectsInput{
			Bucket:  d.Bucket,
			Quiet:   false,
			Objects: obsObjects[i:min(i+1000, total)],
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	methodString := obs.HTTP_GET
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != obs.HTTP_GET && methodString != obs.HTTP_HEAD) {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}

	expiresIn := 2000000 * time.Minute
	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresIn = et.Sub(time.Now())
		}
	}

	output, err := d.Client.CreateSignedUrl(&obs.CreateSignedUrlInput{
		Method:  obs.HttpMethodType(methodString),
		Bucket:  d.Bucket,
		Key:     d.obsPath(path),
		Expires: int(expiresIn) * 600,
	})
	return output.SignedUrl, err
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

func contains(arr []obs.AclType, objectAcl obs.AclType) bool {
	for _, o := range arr {
		if objectAcl == o {
			return true
		}
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (d *driver) obsPath(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.RootDirectory, "/")+path, "/")
}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}

func (d *driver) getACL() obs.AclType {
	return obs.AclType(d.ObjectACL)
}

func (d *driver) getStorageClass() obs.StorageClassType {
	return obs.StorageClassType(d.StorageClass)
}

func parseError(path string, err error) error {
	if obsErr, ok := err.(obs.ObsError); ok && obsErr.Code == "NoSuchKey" {
		return storagedriver.PathNotFoundError{Path: path}
	}

	return err
}

// writer attempts to upload parts to OBS in a buffered fashion where the last
// part is at least as large as the chunksize, so the multipart upload could be
// cleanly resumed in the future. This is violated if Close is called after less
// than a full chunk is written.
type writer struct {
	driver      *driver
	key         string
	uploadID    string
	parts       []obs.Part
	size        int64
	readyPart   []byte
	pendingPart []byte
	closed      bool
	committed   bool
	cancelled   bool
}

func (d *driver) newWriter(key string, uploadID string, parts []obs.Part) storagedriver.FileWriter {
        var size int64
	for _, part := range parts {
		size += part.Size
	}
	return &writer{
		driver:   d,
		key:      key,
		uploadID: uploadID,
		parts:    parts,
		size:     size,
	}
}

type completedParts []obs.Part

func (a completedParts) Len() int {
	return len(a)
}

func (a completedParts) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a completedParts) Less(i, j int) bool {
	return a[i].PartNumber < a[j].PartNumber
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	// If the last written part is smaller than minChunkSize, we need to make a
	// new multipart upload :sadface:
	if len(w.parts) > 0 && int(w.parts[len(w.parts)-1].Size) < minChunkSize {
		var completedUploadedParts completedParts
		for _, part := range w.parts {
			completedUploadedParts = append(completedUploadedParts, obs.Part{
				ETag:       part.ETag,
				PartNumber: part.PartNumber,
			})
		}

		sort.Sort(completedUploadedParts)

		_, err := w.driver.Client.CompleteMultipartUpload(&obs.CompleteMultipartUploadInput{
			Bucket:   w.driver.Bucket,
			Key:      w.key,
			UploadId: w.uploadID,
			Parts:    completedUploadedParts,
		})
		if err != nil {
			w.driver.Client.AbortMultipartUpload(&obs.AbortMultipartUploadInput{
				Bucket:   w.driver.Bucket,
				Key:      w.key,
				UploadId: w.uploadID,
			})
			return 0, err
		}

		output, err := w.driver.Client.InitiateMultipartUpload(&obs.InitiateMultipartUploadInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket:       w.driver.Bucket,
				Key:          w.key,
				ACL:          w.driver.getACL(),
				StorageClass: w.driver.getStorageClass(),
				//SseHeader:    obs.SseKmsHeader{Encryption: obs.DEFAULT_SSE_KMS_ENCRYPTION},
			},
			ContentType: w.driver.getContentType(),
		})
		if err != nil {
			return 0, err
		}
		w.uploadID = output.UploadId

		// If the entire written file is smaller than minChunkSize, we need to make
		// a new part from scratch :double sad face:
		if w.size < minChunkSize {
			output, err := w.driver.Client.GetObject(&obs.GetObjectInput{
				GetObjectMetadataInput: obs.GetObjectMetadataInput{
					Bucket: w.driver.Bucket,
					Key:    w.key,
				},
			})
			defer output.Body.Close()
			if err != nil {
				return 0, err
			}

			w.parts = nil
			w.readyPart, err = ioutil.ReadAll(output.Body)
			if err != nil {
				return 0, err
			}
		} else {
			// Otherwise we can use the old file as the new first part
			copyPartOutput, err := w.driver.Client.CopyPart(&obs.CopyPartInput{
					Bucket: w.driver.Bucket,
                                        Key: w.key,
                                        UploadId: output.UploadId,
                                        PartNumber: 1,
                                        CopySourceBucket: w.driver.Bucket,
                                        CopySourceKey: w.driver.Bucket + "/" + w.key,
			})
			if err != nil {
				return 0, err
			}

			w.parts = append(w.parts, obs.Part{
				ETag:       copyPartOutput.ETag,
				PartNumber: 1,
				Size:       w.size,
			})
		}
	}

	var n int

	for len(p) > 0 {
		// If no parts are ready to write, fill up the first part
		if neededBytes := int(w.driver.ChunkSize) - len(w.readyPart); neededBytes > 0 {
			if len(p) >= neededBytes {
				w.readyPart = append(w.readyPart, p[:neededBytes]...)
				n += neededBytes
				p = p[neededBytes:]
			} else {
				w.readyPart = append(w.readyPart, p...)
				n += len(p)
				p = nil
			}
		}

		if neededBytes := int(w.driver.ChunkSize) - len(w.pendingPart); neededBytes > 0 {
			if len(p) >= neededBytes {
				w.pendingPart = append(w.pendingPart, p[:neededBytes]...)
				n += neededBytes
				p = p[neededBytes:]
				err := w.flushPart()
				if err != nil {
					w.size += int64(n)
					return n, err
				}
			} else {
				w.pendingPart = append(w.pendingPart, p...)
				n += len(p)
				p = nil
			}
		}
	}
	w.size += int64(n)
	return n, nil
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	return w.flushPart()
}

func (w *writer) Cancel() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.cancelled = true
	_, err := w.driver.Client.AbortMultipartUpload(&obs.AbortMultipartUploadInput{
		Bucket:   w.driver.Bucket,
		Key:      w.key,
		UploadId: w.uploadID,
	})
	return err
}

func (w *writer) Commit() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	err := w.flushPart()
	if err != nil {
		return err
	}
	w.committed = true

	var completedUploadedParts completedParts
	for _, part := range w.parts {
		completedUploadedParts = append(completedUploadedParts, obs.Part{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
	}

	sort.Sort(completedUploadedParts)

	_, err = w.driver.Client.CompleteMultipartUpload(&obs.CompleteMultipartUploadInput{
		Bucket:   w.driver.Bucket,
		Key:      w.key,
		UploadId: w.uploadID,
		Parts:    completedUploadedParts,
	})
	if err != nil {
		w.driver.Client.AbortMultipartUpload(&obs.AbortMultipartUploadInput{
			Bucket:   w.driver.Bucket,
			Key:      w.key,
			UploadId: w.uploadID,
		})
		return err
	}
	return nil
}

// flushPart flushes buffers to write a part to OBS.
// Only called by Write (with both buffers full) and Close/Commit (always)
func (w *writer) flushPart() error {
	if len(w.readyPart) == 0 && len(w.pendingPart) == 0 {
		// nothing to write
		return nil
	}
	if len(w.pendingPart) < int(w.driver.ChunkSize) {
		// closing with a small pending part
		// combine ready and pending to avoid writing a small part
		w.readyPart = append(w.readyPart, w.pendingPart...)
		w.pendingPart = nil
	}

	partNumber := len(w.parts) + 1
	output, err := w.driver.Client.UploadPart(&obs.UploadPartInput{
		Bucket:     w.driver.Bucket,
		Key:        w.key,
		PartNumber: partNumber,
		UploadId:   w.uploadID,
		Body:       bytes.NewReader(w.readyPart),
	})
	if err != nil {
		return err
	}

	w.parts = append(w.parts, obs.Part{
		ETag:       output.ETag,
		PartNumber: partNumber,
		Size:       int64(len(w.readyPart)),
	})
	w.readyPart = w.pendingPart
	w.pendingPart = nil
	return nil
}

//func (d *driver) getEncryptionMode() string {
//	if !d.Encrypt {
//		return nil
//	}
//	if d.KeyID == "" {
//		return obs.DEFAULT_SSE_C_ENCRYPTION
//	}
//	return obs.DEFAULT_SSE_KMS_ENCRYPTION
//}
//
//func (d *driver) getSSEKMSKeyID() string {
//	if d.KeyID != "" {
//		return d.KeyID
//	}
//	return ""
//}
