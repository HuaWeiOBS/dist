//
// +build include_obs

package obs

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"eSDK_Storage_OBS_V2.2.1_Go/src/obs"
	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	"gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

var obsDriverConstructor func(rootDirectory string, storageClass obs.StorageClassType) (*Driver, error)
var skipOBS func() string

func init() {
	accessKey := os.Getenv("HUAWEI_CLOUD_ACCESS_KEY")
	secretKey := os.Getenv("HUAWEI_CLOUD_SECRET_KEY")
	bucket := os.Getenv("OBS_BUCKET")
	endpoint := os.Getenv("OBS_ENDPOINT")
	secure := os.Getenv("OBS_SECURE")
	v2Auth := os.Getenv("OBS_V2_AUTH")
	objectACL := os.Getenv("OBS_OBJECT_ACL")
	//encrypt := os.Getenv("OBS_ENCRYPT")
	//keyID := os.Getenv("OBS_KEY_ID")
	//securityToken := os.Getenv("HUAWEI_CLOUD_SESSION_TOKEN")
	pathStyle := os.Getenv("OBS_PATHSTYLE")
	root, err := ioutil.TempDir("/temp/", "driver-")
	if err != nil {
		panic(err)
	}
	defer os.Remove(root)

	obsDriverConstructor = func(rootDirectory string, storageClass obs.StorageClassType) (*Driver, error) {
		//encryptBool := false
		//if encrypt != "" {
		//	encryptBool, err = strconv.ParseBool(encrypt)
		//	if err != nil {
		//		return nil, err
		//	}
		//}

		secureBool := false
		if secure != "" {
			secureBool, err = strconv.ParseBool(secure)
			if err != nil {
				return nil, err
			}
		}

		v2 := obs.SignatureV2
		if v2Auth != "" {
			v2 = obs.SignatureType(v2Auth)
			if !(v2 == obs.SignatureV2 || v2 == obs.SignatureV4) {
				return nil, fmt.Errorf("The v2auth parameter should be either v2 or v4")
			}
		}

		acl := obs.AclType(objectACL)

		pathStyleBool := false
		if pathStyle != "" {
			pathStyleBool, err = strconv.ParseBool(pathStyle)
			if err != nil {
				return nil, err
			}
		}

		parameters := DriverParameters{
			accessKey,
			secretKey,
			bucket,
			region,
			endpoint,
			secureBool,
			v2,
			minChunkSize,
			defaultMultipartCopyChunkSize,
			defaultMultipartCopyMaxConcurrency,
			defaultMultipartCopyThresholdSize,
			rootDirectory,
			storageClass,
			acl,
			//encryptBool,
			//keyID,
			//securityToken,
			pathStyleBool,
		}

		return New(parameters)
	}

	// Skip OBS storage driver tests if environment variable parameters are not provided
	skipOBS = func() string {
		if accessKey == "" || secretKey == "" || bucket == "" || endpoint == "" || region == "" {
			return "Must set HUAWEI_CLOUD_ACCESS_KEY, HUAWEI_CLOUD_SECRET_KEY, OBS_BUCKET, OBS_ENDPOINT and OBS_REGION to run OBS tests"
		}
		return ""
	}

	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return obsDriverConstructor(root, obs.StorageClassStandard)
	}, skipOBS)

}

func TestEmptyRootList(t *testing.T) {
	if skipOBS() != "" {
		t.Skip(skipOBS())
	}

	validRoot, err := ioutil.TempDir("/temp/", "driver-")
	if err != nil {
		t.Fatalf("unexpected error creating temporary directory: %v", err)
	}
	defer os.Remove(validRoot)

	rootedDriver, err := obsDriverConstructor(validRoot, obs.StorageClassStandard)
	if err != nil {
		t.Fatalf("unexpected error creating rooted driver: %v", err)
	}

	emptyRootDriver, err := obsDriverConstructor("", obs.StorageClassStandard)
	if err != nil {
		t.Fatalf("unexpected error creating empty root driver: %v", err)
	}

	slashRootDriver, err := obsDriverConstructor("/", obs.StorageClassStandard)
	if err != nil {
		t.Fatalf("unexpected error creating slash root driver: %v", err)
	}

	filename := "/test"
	contents := []byte("contents")
	ctx := context.Background()
	err = rootedDriver.PutContent(ctx, filename, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}
	defer rootedDriver.Delete(ctx, filename)

	keys, err := emptyRootDriver.List(ctx, "/")
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}
	fmt.Println("Files in emptyRootDriver: ", keys)

	keys, err = slashRootDriver.List(ctx, "/")
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}
	fmt.Println("Files in slashRootDriver: ", keys)
}

func TestStorageClass(t *testing.T) {
	if skipOBS() != "" {
		t.Skip(skipOBS())
	}

	rootDir, err := ioutil.TempDir("", "driver-")
	if err != nil {
		t.Fatalf("unexpected error creating temporary directory: %v", err)
	}
	defer os.Remove(rootDir)

	standardDriver, err := obsDriverConstructor(rootDir, obs.StorageClassStandard)
	if err != nil {
		t.Fatalf("unexpected error creating driver with standard storage: %v", err)
	}

	if _, err = obsDriverConstructor(rootDir, noStorageClass); err != nil {
		t.Fatalf("unexpected error creating driver without storage class: %v", err)
	}

	standardFilename := "/test-standard" // sourcePath
	contents := []byte("contents")
	ctx := context.Background()

	err = standardDriver.PutContent(ctx, standardFilename, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}
	defer standardDriver.Delete(ctx, standardFilename)

	standardDriverUnwrapped := standardDriver.Base.StorageDriver.(*driver)
	output, err := standardDriverUnwrapped.Client.GetObject(&obs.GetObjectInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: standardDriverUnwrapped.Bucket,
			Key:    standardDriverUnwrapped.obsPath(standardFilename),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error retrieving standard storage file: %v", err)
	}

	defer output.Body.Close()

	// Huawei OBS only populates this header value for non-standard storage classes
	if output.StorageClass != "" {
		t.Fatalf("unexpected storage class for standard file: %v", output.StorageClass)
	}
}

func TestOverThousandBlobs(t *testing.T) {
	if skipOBS() != "" {
		t.Skip(skipOBS())
	}

	rootDir, err := ioutil.TempDir("", "driver-")
	if err != nil {
		t.Fatalf("unexpected error creating temporary directory: %v", err)
	}
	defer os.Remove(rootDir)

	standardDriver, err := obsDriverConstructor(rootDir, obs.StorageClassStandard)
	if err != nil {
		t.Fatalf("unexpected error creating driver with standard storage: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 1005; i++ {
		filename := "/test_1000_blobs/file" + strconv.Itoa(i)
		contents := []byte("contents")
		err = standardDriver.PutContent(ctx, filename, contents)
		if err != nil {
			t.Fatalf("unexpected error creating content: %v", err)
		}
	}

	// cant actually verify deletion because read-after-delete is inconsistent, but can ensure no errors
	err = standardDriver.Delete(ctx, "/test_1000_blobs")
	if err != nil {
		t.Fatalf("unexpected error deleting thousand files: %v", err)
	}
}

func TestMoveWithMultipartCopy(t *testing.T) {
	if skipOBS() != "" {
		t.Skip(skipOBS())
	}

	rootDir, err := ioutil.TempDir("", "driver-")
	if err != nil {
		t.Fatalf("unexpected error creating temporary directory: %v", err)
	}
	defer os.Remove(rootDir)

	d, err := obsDriverConstructor(rootDir, obs.StorageClassStandard)
	if err != nil {
		t.Fatalf("unexpected error creating driver with standard storage: %v", err)
	}

	ctx := context.Background()
	sourcePath := "/source"
	destPath := "/des"

	defer d.Delete(ctx, sourcePath)
	defer d.Delete(ctx, destPath)

	//An object larger than d's MultipartCopyThresholdSize will cause d.Move() to perform a multipart copy.
	multipartCopyThresholdSize := d.baseEmbed.Base.StorageDriver.(*driver).MultipartCopyThresholdSize
	contents := make([]byte, multipartCopyThresholdSize)
	rand.Read(contents)

	err = d.PutContent(ctx, sourcePath, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}

	err = d.Move(ctx, sourcePath, destPath)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}

	received, err := d.GetContent(ctx, destPath)
	if err != nil {
		t.Fatalf("unexpected error getting content: %v", err)
	}
	if !bytes.Equal(contents, received) {
		t.Fatal("content differs")
	}

	_, err = d.GetContent(ctx, sourcePath)
	switch err.(type) {
	case storagedriver.PathNotFoundError:
	default:
		t.Fatalf("unexpected error getting content: %v", err)
	}
}
