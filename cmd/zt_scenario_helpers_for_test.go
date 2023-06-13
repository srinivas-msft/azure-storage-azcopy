// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	gcpUtils "cloud.google.com/go/storage"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/minio/minio-go"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
)

const defaultFileSize = 1024

type scenarioHelper struct{}

var specialNames = []string{
	"打麻将.txt",
	"wow such space so much space",
	"打%%#%@#%麻将.txt",
	// "saywut.pdf?yo=bla&WUWUWU=foo&sig=yyy", // TODO this breaks on windows, figure out a way to add it only for tests on Unix
	"coração",
	"আপনার নাম কি",
	"%4509%4254$85140&",
	"Donaudampfschifffahrtselektrizitätenhauptbetriebswerkbauunterbeamtengesellschaft",
	"お名前は何ですか",
	"Adın ne",
	"як вас звати",
}

// note: this is to emulate the list-of-files flag
func (scenarioHelper) generateListOfFiles(a *assert.Assertions, fileList []string) (path string) {
	parentDirName, err := os.MkdirTemp("", "AzCopyLocalTest")
	a.Nil(err)

	// create the file
	path = common.GenerateFullPath(parentDirName, generateName("listy", 0))
	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	a.Nil(err)

	// pipe content into it
	content := strings.Join(fileList, "\n")
	err = os.WriteFile(path, []byte(content), common.DEFAULT_FILE_PERM)
	a.Nil(err)
	return
}

func (scenarioHelper) generateLocalDirectory(a *assert.Assertions) (dstDirName string) {
	dstDirName, err := os.MkdirTemp("", "AzCopyLocalTest")
	a.Nil(err)
	return
}

// create a test file
func (scenarioHelper) generateLocalFile(filePath string, fileSize int) ([]byte, error) {
	// generate random data
	_, bigBuff := getRandomDataAndReader(fileSize)

	// create all parent directories
	err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		return nil, err
	}

	// write to file and return the data
	err = os.WriteFile(filePath, bigBuff, common.DEFAULT_FILE_PERM)
	return bigBuff, err
}

func (s scenarioHelper) generateLocalFilesFromList(a *assert.Assertions, dirPath string, fileList []string) {
	for _, fileName := range fileList {
		_, err := s.generateLocalFile(filepath.Join(dirPath, fileName), defaultFileSize)
		a.Nil(err)
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (s scenarioHelper) generateCommonRemoteScenarioForLocal(a *assert.Assertions, dirPath string, prefix string) (fileList []string) {
	fileList = make([]string, 50)
	for i := 0; i < 10; i++ {
		batch := []string{
			generateName(prefix+"top", 0),
			generateName(prefix+"sub1/", 0),
			generateName(prefix+"sub2/", 0),
			generateName(prefix+"sub1/sub3/sub5/", 0),
			generateName(prefix+specialNames[i], 0),
		}

		for j, name := range batch {
			fileList[5*i+j] = name
			_, err := s.generateLocalFile(filepath.Join(dirPath, name), defaultFileSize)
			a.Nil(err)
		}
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

func (scenarioHelper) generateCommonRemoteScenarioForSoftDelete(a *assert.Assertions, containerURL azblob.ContainerURL, prefix string) (string, []azblob.BlockBlobURL, []string) {
	blobList := make([]azblob.BlockBlobURL, 3)
	blobNames := make([]string, 3)
	var listOfTransfers []string

	blobURL1, blobName1 := createNewBlockBlob(a, containerURL, prefix+"top")
	blobURL2, blobName2 := createNewBlockBlob(a, containerURL, prefix+"sub1/")
	blobURL3, blobName3 := createNewBlockBlob(a, containerURL, prefix+"sub1/sub3/sub5/")

	blobList[0] = blobURL1
	blobNames[0] = blobName1
	blobList[1] = blobURL2
	blobNames[1] = blobName2
	blobList[2] = blobURL3
	blobNames[2] = blobName3

	for i := 0; i < len(blobList); i++ {
		for j := 0; j < 3; j++ { // create 3 soft-deleted snapshots for each blob
			// Create snapshot for blob
			snapResp, err := blobList[i].CreateSnapshot(ctx, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
			a.NotNil(snapResp)
			a.Nil(err)

			time.Sleep(time.Millisecond * 30)

			// Soft delete snapshot
			snapshotBlob := blobList[i].WithSnapshot(snapResp.Snapshot())
			_, err = snapshotBlob.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			a.Nil(err)

			listOfTransfers = append(listOfTransfers, blobNames[i])
		}
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return blobName1, blobList, listOfTransfers
}

func (scenarioHelper) generateCommonRemoteScenarioForBlob(a *assert.Assertions, containerURL azblob.ContainerURL, prefix string) (blobList []string) {
	blobList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, blobName1 := createNewBlockBlob(a, containerURL, prefix+"top")
		_, blobName2 := createNewBlockBlob(a, containerURL, prefix+"sub1/")
		_, blobName3 := createNewBlockBlob(a, containerURL, prefix+"sub2/")
		_, blobName4 := createNewBlockBlob(a, containerURL, prefix+"sub1/sub3/sub5/")
		_, blobName5 := createNewBlockBlob(a, containerURL, prefix+specialNames[i])

		blobList[5*i] = blobName1
		blobList[5*i+1] = blobName2
		blobList[5*i+2] = blobName3
		blobList[5*i+3] = blobName4
		blobList[5*i+4] = blobName5
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

// same as blob, but for every virtual directory, a blob with the same name is created, and it has metadata 'hdi_isfolder = true'
func (scenarioHelper) generateCommonRemoteScenarioForWASB(a *assert.Assertions, containerURL azblob.ContainerURL, prefix string) (blobList []string) {
	blobList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, blobName1 := createNewBlockBlob(a, containerURL, prefix+"top")
		_, blobName2 := createNewBlockBlob(a, containerURL, prefix+"sub1/")
		_, blobName3 := createNewBlockBlob(a, containerURL, prefix+"sub2/")
		_, blobName4 := createNewBlockBlob(a, containerURL, prefix+"sub1/sub3/sub5/")
		_, blobName5 := createNewBlockBlob(a, containerURL, prefix+specialNames[i])

		blobList[5*i] = blobName1
		blobList[5*i+1] = blobName2
		blobList[5*i+2] = blobName3
		blobList[5*i+3] = blobName4
		blobList[5*i+4] = blobName5
	}

	if prefix != "" {
		rootDir := strings.TrimSuffix(prefix, "/")
		createNewDirectoryStub(a, containerURL, rootDir)
		blobList = append(blobList, rootDir)
	}

	createNewDirectoryStub(a, containerURL, prefix+"sub1")
	createNewDirectoryStub(a, containerURL, prefix+"sub1/sub3")
	createNewDirectoryStub(a, containerURL, prefix+"sub1/sub3/sub5")
	createNewDirectoryStub(a, containerURL, prefix+"sub2")

	for _, dirPath := range []string{prefix + "sub1", prefix + "sub1/sub3", prefix + "sub1/sub3/sub5", prefix + "sub2"} {
		blobList = append(blobList, dirPath)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

func (scenarioHelper) generateCommonRemoteScenarioForBlobFS(a *assert.Assertions, filesystemURL azbfs.FileSystemURL, prefix string) (pathList []string) {
	pathList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, pathName1 := createNewBfsFile(a, filesystemURL, prefix+"top")
		_, pathName2 := createNewBfsFile(a, filesystemURL, prefix+"sub1/")
		_, pathName3 := createNewBfsFile(a, filesystemURL, prefix+"sub2/")
		_, pathName4 := createNewBfsFile(a, filesystemURL, prefix+"sub1/sub3/sub5")
		_, pathName5 := createNewBfsFile(a, filesystemURL, prefix+specialNames[i])

		pathList[5*i] = pathName1
		pathList[5*i+1] = pathName2
		pathList[5*i+2] = pathName3
		pathList[5*i+3] = pathName4
		pathList[5*i+4] = pathName5
	}

	// sleep a bit so that the paths' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1500)
	return
}

func (scenarioHelper) generateCommonRemoteScenarioForAzureFile(a *assert.Assertions, shareURL azfile.ShareURL, prefix string) (fileList []string) {
	fileList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, fileName1 := createNewAzureFile(a, shareURL, prefix+"top")
		_, fileName2 := createNewAzureFile(a, shareURL, prefix+"sub1/")
		_, fileName3 := createNewAzureFile(a, shareURL, prefix+"sub2/")
		_, fileName4 := createNewAzureFile(a, shareURL, prefix+"sub1/sub3/sub5/")
		_, fileName5 := createNewAzureFile(a, shareURL, prefix+specialNames[i])

		fileList[5*i] = fileName1
		fileList[5*i+1] = fileName2
		fileList[5*i+2] = fileName3
		fileList[5*i+3] = fileName4
		fileList[5*i+4] = fileName5
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

func (s scenarioHelper) generateBlobContainersAndBlobsFromLists(a *assert.Assertions, serviceURL azblob.ServiceURL, containerList []string, blobList []string, data string) {
	for _, containerName := range containerList {
		curl := serviceURL.NewContainerURL(containerName)
		_, err := curl.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		a.Nil(err)

		s.generateBlobsFromList(a, curl, blobList, data)
	}
}

func (s scenarioHelper) generateFileSharesAndFilesFromLists(a *assert.Assertions, serviceURL azfile.ServiceURL, shareList []string, fileList []string, data string) {
	for _, shareName := range shareList {
		surl := serviceURL.NewShareURL(shareName)
		_, err := surl.Create(ctx, azfile.Metadata{}, 0)
		a.Nil(err)

		s.generateAzureFilesFromList(a, surl, fileList)
	}
}

func (s scenarioHelper) generateFilesystemsAndFilesFromLists(a *assert.Assertions, serviceURL azbfs.ServiceURL, fsList []string, fileList []string, data string) {
	for _, filesystemName := range fsList {
		fsURL := serviceURL.NewFileSystemURL(filesystemName)
		_, err := fsURL.Create(ctx)
		a.Nil(err)

		s.generateBFSPathsFromList(a, fsURL, fileList)
	}
}

func (s scenarioHelper) generateS3BucketsAndObjectsFromLists(a *assert.Assertions, s3Client *minio.Client, bucketList []string, objectList []string, data string) {
	for _, bucketName := range bucketList {
		err := s3Client.MakeBucket(bucketName, "")
		a.Nil(err)

		s.generateObjects(a, s3Client, bucketName, objectList)
	}
}

func (s scenarioHelper) generateGCPBucketsAndObjectsFromLists(a *assert.Assertions, client *gcpUtils.Client, bucketList []string, objectList []string) {
	for _, bucketName := range bucketList {
		bkt := client.Bucket(bucketName)
		err := bkt.Create(context.Background(), os.Getenv("GOOGLE_CLOUD_PROJECT"), &gcpUtils.BucketAttrs{})
		a.Nil(err)
		s.generateGCPObjects(a, client, bucketName, objectList)
	}
}

// create the demanded blobs
func (scenarioHelper) generateBlobsFromList(a *assert.Assertions, containerURL azblob.ContainerURL, blobList []string, data string) {
	for _, blobName := range blobList {
		blob := containerURL.NewBlockBlobURL(blobName)
		_, err := blob.Upload(ctx, strings.NewReader(data), azblob.BlobHTTPHeaders{},
			nil, azblob.BlobAccessConditions{}, azblob.DefaultAccessTier, nil, azblob.ClientProvidedKeyOptions{}, azblob.ImmutabilityPolicyOptions{})
		a.Nil(err)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generatePageBlobsFromList(a *assert.Assertions, containerURL azblob.ContainerURL, blobList []string, data string) {
	for _, blobName := range blobList {
		// Create the blob (PUT blob)
		blob := containerURL.NewPageBlobURL(blobName)
		_, err := blob.Create(ctx,
			int64(len(data)),
			0,
			azblob.BlobHTTPHeaders{
				ContentType: "text/random",
			},
			azblob.Metadata{},
			azblob.BlobAccessConditions{},
			azblob.DefaultPremiumBlobAccessTier,
			nil,
			azblob.ClientProvidedKeyOptions{},
			azblob.ImmutabilityPolicyOptions{},
		)
		a.Nil(err)

		// Create the page (PUT page)
		_, err = blob.UploadPages(ctx,
			0,
			strings.NewReader(data),
			azblob.PageBlobAccessConditions{},
			nil,
			azblob.ClientProvidedKeyOptions{},
		)
		a.Nil(err)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generateAppendBlobsFromList(a *assert.Assertions, containerURL azblob.ContainerURL, blobList []string, data string) {
	for _, blobName := range blobList {
		// Create the blob (PUT blob)
		blob := containerURL.NewAppendBlobURL(blobName)
		_, err := blob.Create(ctx,
			azblob.BlobHTTPHeaders{
				ContentType: "text/random",
			},
			azblob.Metadata{},
			azblob.BlobAccessConditions{},
			nil,
			azblob.ClientProvidedKeyOptions{},
			azblob.ImmutabilityPolicyOptions{},
		)
		a.Nil(err)

		// Append a block (PUT block)
		_, err = blob.AppendBlock(ctx,
			strings.NewReader(data),
			azblob.AppendBlobAccessConditions{},
			nil,
			azblob.ClientProvidedKeyOptions{})
		a.Nil(err)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generateBlockBlobWithAccessTier(a *assert.Assertions, containerURL azblob.ContainerURL, blobName string, accessTier azblob.AccessTierType) {
	blob := containerURL.NewBlockBlobURL(blobName)
	_, err := blob.Upload(ctx, strings.NewReader(blockBlobDefaultData), azblob.BlobHTTPHeaders{},
		nil, azblob.BlobAccessConditions{}, accessTier, nil, azblob.ClientProvidedKeyOptions{}, azblob.ImmutabilityPolicyOptions{})
	a.Nil(err)
}

// create the demanded objects
func (scenarioHelper) generateObjects(a *assert.Assertions, client *minio.Client, bucketName string, objectList []string) {
	size := int64(len(objectDefaultData))
	for _, objectName := range objectList {
		n, err := client.PutObjectWithContext(ctx, bucketName, objectName, strings.NewReader(objectDefaultData), size, minio.PutObjectOptions{})
		a.Nil(err)
		a.Equal(size, n)
	}
}

func (scenarioHelper) generateGCPObjects(a *assert.Assertions, client *gcpUtils.Client, bucketName string, objectList []string) {
	size := int64(len(objectDefaultData))
	for _, objectName := range objectList {
		wc := client.Bucket(bucketName).Object(objectName).NewWriter(context.Background())
		reader := strings.NewReader(objectDefaultData)
		written, err := io.Copy(wc, reader)
		a.Nil(err)
		a.Equal(size, written)
		err = wc.Close()
		a.Nil(err)
	}
}

// create the demanded files
func (scenarioHelper) generateFlatFiles(a *assert.Assertions, shareURL azfile.ShareURL, fileList []string) {
	for _, fileName := range fileList {
		file := shareURL.NewRootDirectoryURL().NewFileURL(fileName)
		err := azfile.UploadBufferToAzureFile(ctx, []byte(fileDefaultData), file, azfile.UploadToAzureFileOptions{})
		a.Nil(err)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

// make 50 objects with random names
// 10 of them at the top level
// 10 of them in sub dir "sub1"
// 10 of them in sub dir "sub2"
// 10 of them in deeper sub dir "sub1/sub3/sub5"
// 10 of them with special characters
func (scenarioHelper) generateCommonRemoteScenarioForS3(a *assert.Assertions, client *minio.Client, bucketName string, prefix string, returnObjectListWithBucketName bool) (objectList []string) {
	objectList = make([]string, 50)

	for i := 0; i < 10; i++ {
		objectName1 := createNewObject(a, client, bucketName, prefix+"top")
		objectName2 := createNewObject(a, client, bucketName, prefix+"sub1/")
		objectName3 := createNewObject(a, client, bucketName, prefix+"sub2/")
		objectName4 := createNewObject(a, client, bucketName, prefix+"sub1/sub3/sub5/")
		objectName5 := createNewObject(a, client, bucketName, prefix+specialNames[i])

		// Note: common.AZCOPY_PATH_SEPARATOR_STRING is added before bucket or objectName, as in the change minimize JobPartPlan file size,
		// transfer.Source & transfer.Destination(after trimming the SourceRoot and DestinationRoot) are with AZCOPY_PATH_SEPARATOR_STRING suffix,
		// when user provided source & destination are without / suffix, which is the case for scenarioHelper generated URL.

		bucketPath := ""
		if returnObjectListWithBucketName {
			bucketPath = common.AZCOPY_PATH_SEPARATOR_STRING + bucketName
		}

		objectList[5*i] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName1
		objectList[5*i+1] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName2
		objectList[5*i+2] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName3
		objectList[5*i+3] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName4
		objectList[5*i+4] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName5
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

func (scenarioHelper) generateCommonRemoteScenarioForGCP(a *assert.Assertions, client *gcpUtils.Client, bucketName string, prefix string, returnObjectListWithBucketName bool) []string {
	objectList := make([]string, 50)
	for i := 0; i < 10; i++ {
		objectName1 := createNewGCPObject(a, client, bucketName, prefix+"top")
		objectName2 := createNewGCPObject(a, client, bucketName, prefix+"sub1/")
		objectName3 := createNewGCPObject(a, client, bucketName, prefix+"sub2/")
		objectName4 := createNewGCPObject(a, client, bucketName, prefix+"sub1/sub3/sub5/")
		objectName5 := createNewGCPObject(a, client, bucketName, prefix+specialNames[i])

		// Note: common.AZCOPY_PATH_SEPARATOR_STRING is added before bucket or objectName, as in the change minimize JobPartPlan file size,
		// transfer.Source & transfer.Destination(after trimming the SourceRoot and DestinationRoot) are with AZCOPY_PATH_SEPARATOR_STRING suffix,
		// when user provided source & destination are without / suffix, which is the case for scenarioHelper generated URL.

		bucketPath := ""
		if returnObjectListWithBucketName {
			bucketPath = common.AZCOPY_PATH_SEPARATOR_STRING + bucketName
		}

		objectList[5*i] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName1
		objectList[5*i+1] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName2
		objectList[5*i+2] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName3
		objectList[5*i+3] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName4
		objectList[5*i+4] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName5
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return objectList
}

// create the demanded azure files
func (scenarioHelper) generateAzureFilesFromList(a *assert.Assertions, shareURL azfile.ShareURL, fileList []string) {
	for _, filePath := range fileList {
		file := shareURL.NewRootDirectoryURL().NewFileURL(filePath)

		// create parents first
		generateParentsForAzureFile(a, file)

		// create the file itself
		cResp, err := file.Create(ctx, defaultAzureFileSizeInBytes, azfile.FileHTTPHeaders{}, azfile.Metadata{})
		a.Nil(err)
		a.Equal(201, cResp.StatusCode())
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generateBFSPathsFromList(a *assert.Assertions, filesystemURL azbfs.FileSystemURL, fileList []string) {
	for _, p := range fileList {
		file := filesystemURL.NewRootDirectoryURL().NewFileURL(p)

		// Create the file
		cResp, err := file.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
		a.Nil(err)
		a.Equal(201, cResp.StatusCode())

		aResp, err := file.AppendData(ctx, 0, strings.NewReader(string(make([]byte, defaultBlobFSFileSizeInBytes))))
		a.Nil(err)
		a.Equal(202, aResp.StatusCode())

		fResp, err := file.FlushData(ctx, defaultBlobFSFileSizeInBytes, nil, azbfs.BlobFSHTTPHeaders{}, false, true)
		a.Nil(err)
		a.Equal(200, fResp.StatusCode())
	}
}

// Golang does not have sets, so we have to use a map to fulfill the same functionality
func (scenarioHelper) convertListToMap(list []string) map[string]int {
	lookupMap := make(map[string]int)
	for _, entryName := range list {
		lookupMap[entryName] = 0
	}

	return lookupMap
}

func (scenarioHelper) convertMapKeysToList(m map[string]int) []string {
	list := make([]string, len(m))
	i := 0
	for key := range m {
		list[i] = key
		i++
	}
	return list
}

// useful for files->files transfers, where folders are included in the transfers.
// includeRoot should be set to true for cases where we expect the root directory to be copied across
// (i.e. where we expect the behaviour that can be, but has not been in this case, turned off by appending /* to the source)
func (s scenarioHelper) addFoldersToList(fileList []string, includeRoot bool) []string {
	m := s.convertListToMap(fileList)
	// for each file, add all its parent dirs
	for name := range m {
		for {
			name = path.Dir(name)
			if name == "." {
				if includeRoot {
					m[""] = 0 // don't use "."
				}
				break
			} else {
				m[name] = 0
			}
		}
	}
	return s.convertMapKeysToList(m)
}

func (scenarioHelper) shaveOffPrefix(list []string, prefix string) []string {
	cleanList := make([]string, len(list))
	for i, item := range list {
		cleanList[i] = strings.TrimPrefix(item, prefix)
	}
	return cleanList
}

func (scenarioHelper) addPrefix(list []string, prefix string) []string {
	modifiedList := make([]string, len(list))
	for i, item := range list {
		modifiedList[i] = prefix + item
	}
	return modifiedList
}

func (scenarioHelper) getRawContainerURLWithSAS(a *assert.Assertions, containerName string) url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	containerURLWithSAS := getContainerURLWithSAS(a, *credential, containerName)
	return containerURLWithSAS.URL()
}

func (scenarioHelper) getRawBlobURLWithSAS(a *assert.Assertions, containerName string, blobName string) url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	containerURLWithSAS := getContainerURLWithSAS(a, *credential, containerName)
	blobURLWithSAS := containerURLWithSAS.NewBlockBlobURL(blobName)
	return blobURLWithSAS.URL()
}

func (scenarioHelper) getRawBlobServiceURLWithSAS(a *assert.Assertions) url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	return getBlobServiceURLWithSAS(a, *credential).URL()
}

func (scenarioHelper) getRawFileServiceURLWithSAS(a *assert.Assertions) url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	return getFileServiceURLWithSAS(a, *credential).URL()
}

func (scenarioHelper) getRawAdlsServiceURLWithSAS(a *assert.Assertions) azbfs.ServiceURL {
	accountName, accountKey := getAccountAndKey()
	credential := azbfs.NewSharedKeyCredential(accountName, accountKey)

	return getAdlsServiceURLWithSAS(a, *credential)
}

func (scenarioHelper) getBlobServiceURL(a *assert.Assertions) azblob.ServiceURL {
	accountName, accountKey := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net", credential.AccountName())

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	return azblob.NewServiceURL(*fullURL, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
}

func (s scenarioHelper) getContainerURL(a *assert.Assertions, containerName string) azblob.ContainerURL {
	serviceURL := s.getBlobServiceURL(a)
	containerURL := serviceURL.NewContainerURL(containerName)

	return containerURL
}

func (scenarioHelper) getRawS3AccountURL(a *assert.Assertions, region string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com", common.IffString(region == "", "", "-"+region))

	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	return *fullURL
}

func (scenarioHelper) getRawGCPAccountURL(a *assert.Assertions) url.URL {
	rawURL := "https://storage.cloud.google.com/"
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)
	return *fullURL
}

// TODO: Possibly add virtual-hosted-style and dual stack support. Currently use path style for testing.
func (scenarioHelper) getRawS3BucketURL(a *assert.Assertions, region string, bucketName string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com/%s", common.IffString(region == "", "", "-"+region), bucketName)

	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	return *fullURL
}

func (scenarioHelper) getRawGCPBucketURL(a *assert.Assertions, bucketName string) url.URL {
	rawURL := fmt.Sprintf("https://storage.cloud.google.com/%s", bucketName)
	fmt.Println(rawURL)
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)
	return *fullURL
}

func (scenarioHelper) getRawS3ObjectURL(a *assert.Assertions, region string, bucketName string, objectName string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com/%s/%s", common.IffString(region == "", "", "-"+region), bucketName, objectName)

	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	return *fullURL
}

func (scenarioHelper) getRawGCPObjectURL(a *assert.Assertions, bucketName string, objectName string) url.URL {
	rawURL := fmt.Sprintf("https://storage.cloud.google.com/%s/%s", bucketName, objectName)
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)
	return *fullURL
}

func (scenarioHelper) getRawFileURLWithSAS(a *assert.Assertions, shareName string, fileName string) url.URL {
	credential, err := getGenericCredentialForFile("")
	a.Nil(err)
	shareURLWithSAS := getShareURLWithSAS(a, *credential, shareName)
	fileURLWithSAS := shareURLWithSAS.NewRootDirectoryURL().NewFileURL(fileName)
	return fileURLWithSAS.URL()
}

func (scenarioHelper) getRawShareURLWithSAS(a *assert.Assertions, shareName string) url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	shareURLWithSAS := getShareURLWithSAS(a, *credential, shareName)
	return shareURLWithSAS.URL()
}

func (scenarioHelper) blobExists(blobURL azblob.BlobURL) bool {
	_, err := blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err == nil {
		return true
	}
	return false
}

func (scenarioHelper) containerExists(containerURL azblob.ContainerURL) bool {
	_, err := containerURL.GetProperties(context.Background(), azblob.LeaseAccessConditions{})
	if err == nil {
		return true
	}
	return false
}

func runSyncAndVerify(a *assert.Assertions, raw rawSyncCmdArgs, verifier func(err error)) {
	// the simulated user input should parse properly
	cooked, err := raw.cook()
	a.Nil(err)

	// the enumeration ends when process() returns
	err = cooked.process()

	// the err is passed to verified, which knows whether it is expected or not
	verifier(err)
}

func runCopyAndVerify(a *assert.Assertions, raw rawCopyCmdArgs, verifier func(err error)) {
	// the simulated user input should parse properly
	cooked, err := raw.cook()
	if err == nil {
		err = cooked.makeTransferEnum()
	}
	if err != nil {
		verifier(err)
		return
	}

	// the enumeration ends when process() returns
	err = cooked.process()

	// the err is passed to verified, which knows whether it is expected or not
	verifier(err)
}

func validateUploadTransfersAreScheduled(a *assert.Assertions, sourcePrefix string, destinationPrefix string, expectedTransfers []string, mockedRPC interceptor) {
	validateCopyTransfersAreScheduled(a, false, true, sourcePrefix, destinationPrefix, expectedTransfers, mockedRPC)
}

func validateDownloadTransfersAreScheduled(a *assert.Assertions, sourcePrefix string, destinationPrefix string, expectedTransfers []string, mockedRPC interceptor) {
	validateCopyTransfersAreScheduled(a, true, false, sourcePrefix, destinationPrefix, expectedTransfers, mockedRPC)
}

func validateS2SSyncTransfersAreScheduled(a *assert.Assertions, sourcePrefix string, destinationPrefix string, expectedTransfers []string, mockedRPC interceptor) {
	validateCopyTransfersAreScheduled(a, true, true, sourcePrefix, destinationPrefix, expectedTransfers, mockedRPC)
}

func validateCopyTransfersAreScheduled(a *assert.Assertions, isSrcEncoded bool, isDstEncoded bool, sourcePrefix string, destinationPrefix string, expectedTransfers []string, mockedRPC interceptor) {
	// validate that the right number of transfers were scheduled
	a.Equal(len(expectedTransfers), len(mockedRPC.transfers))

	// validate that the right transfers were sent
	lookupMap := scenarioHelper{}.convertListToMap(expectedTransfers)
	for _, transfer := range mockedRPC.transfers {
		srcRelativeFilePath := strings.TrimPrefix(transfer.Source, sourcePrefix)
		dstRelativeFilePath := strings.TrimPrefix(transfer.Destination, destinationPrefix)

		if isSrcEncoded {
			srcRelativeFilePath, _ = url.PathUnescape(srcRelativeFilePath)

			if runtime.GOOS == "windows" {
				// Decode unsafe dst characters on windows
				pathParts := strings.Split(dstRelativeFilePath, "/")
				invalidChars := `<>\/:"|?*` + string(rune(0x00))

				for _, c := range strings.Split(invalidChars, "") {
					for k, p := range pathParts {
						pathParts[k] = strings.ReplaceAll(p, url.PathEscape(c), c)
					}
				}

				dstRelativeFilePath = strings.Join(pathParts, "/")
			}
		}

		if isDstEncoded {
			dstRelativeFilePath, _ = url.PathUnescape(dstRelativeFilePath)
		}

		// the relative paths should be equal
		a.Equal(dstRelativeFilePath, srcRelativeFilePath)

		// look up the path from the expected transfers, make sure it exists
		_, transferExist := lookupMap[srcRelativeFilePath]
		a.True(transferExist)
	}
}

func validateRemoveTransfersAreScheduled(a *assert.Assertions, isSrcEncoded bool, expectedTransfers []string, mockedRPC interceptor) {

	// validate that the right number of transfers were scheduled
	a.Equal(len(expectedTransfers), len(mockedRPC.transfers))

	// validate that the right transfers were sent
	lookupMap := scenarioHelper{}.convertListToMap(expectedTransfers)
	for _, transfer := range mockedRPC.transfers {
		srcRelativeFilePath := transfer.Source

		if isSrcEncoded {
			srcRelativeFilePath, _ = url.PathUnescape(srcRelativeFilePath)
		}

		// look up the source from the expected transfers, make sure it exists
		_, srcExist := lookupMap[srcRelativeFilePath]
		a.True(srcExist)

		delete(lookupMap, srcRelativeFilePath)
	}
	// if len(lookupMap) > 0 {
	//	panic("set breakpoint here to debug")
	// }
}

func getDefaultSyncRawInput(sra, dst string) rawSyncCmdArgs {
	deleteDestination := common.EDeleteDestination.True()

	return rawSyncCmdArgs{
		src:                 sra,
		dst:                 dst,
		recursive:           true,
		deleteDestination:   deleteDestination.String(),
		md5ValidationOption: common.DefaultHashValidationOption.String(),
		compareHash:         common.ESyncHashType.None().String(),
		localHashStorageMode: common.EHashStorageMode.Default().String(),
	}
}

func getDefaultCopyRawInput(src string, dst string) rawCopyCmdArgs {
	return rawCopyCmdArgs{
		src:                            src,
		dst:                            dst,
		blobType:                       common.EBlobType.Detect().String(),
		blockBlobTier:                  common.EBlockBlobTier.None().String(),
		pageBlobTier:                   common.EPageBlobTier.None().String(),
		md5ValidationOption:            common.DefaultHashValidationOption.String(),
		s2sInvalidMetadataHandleOption: defaultS2SInvalideMetadataHandleOption.String(),
		forceWrite:                     common.EOverwriteOption.True().String(),
		preserveOwner:                  common.PreserveOwnerDefault,
		asSubdir:                       true,
	}
}

func getDefaultRemoveRawInput(src string) rawCopyCmdArgs {
	fromTo := common.EFromTo.BlobTrash()
	srcURL, _ := url.Parse(src)

	if strings.Contains(srcURL.Host, "file") {
		fromTo = common.EFromTo.FileTrash()
	} else if strings.Contains(srcURL.Host, "dfs") {
		fromTo = common.EFromTo.BlobFSTrash()
	}

	return rawCopyCmdArgs{
		src:                            src,
		fromTo:                         fromTo.String(),
		blobType:                       common.EBlobType.Detect().String(),
		blockBlobTier:                  common.EBlockBlobTier.None().String(),
		pageBlobTier:                   common.EPageBlobTier.None().String(),
		md5ValidationOption:            common.DefaultHashValidationOption.String(),
		s2sInvalidMetadataHandleOption: defaultS2SInvalideMetadataHandleOption.String(),
		forceWrite:                     common.EOverwriteOption.True().String(),
		preserveOwner:                  common.PreserveOwnerDefault,
		includeDirectoryStubs:          true,
	}
}

func getDefaultSetPropertiesRawInput(src string, params transferParams) rawCopyCmdArgs {
	fromTo := common.EFromTo.BlobNone()
	srcURL, _ := url.Parse(src)

	srcLocationType := InferArgumentLocation(src)
	switch srcLocationType {
	case common.ELocation.Blob():
		fromTo = common.EFromTo.BlobNone()
	case common.ELocation.BlobFS():
		fromTo = common.EFromTo.BlobFSNone()
	case common.ELocation.File():
		fromTo = common.EFromTo.FileNone()
	default:
		panic(fmt.Sprintf("invalid source type %s to delete. azcopy support removing blobs/files/adls gen2", srcLocationType.String()))

	}

	if strings.Contains(srcURL.Host, "file") {
		fromTo = common.EFromTo.FileNone()
	} else if strings.Contains(srcURL.Host, "dfs") {
		fromTo = common.EFromTo.BlobFSNone()
	}

	rawArgs := rawCopyCmdArgs{
		src:                            src,
		fromTo:                         fromTo.String(),
		blobType:                       common.EBlobType.Detect().String(),
		blockBlobTier:                  common.EBlockBlobTier.None().String(),
		pageBlobTier:                   common.EPageBlobTier.None().String(),
		md5ValidationOption:            common.DefaultHashValidationOption.String(),
		s2sInvalidMetadataHandleOption: defaultS2SInvalideMetadataHandleOption.String(),
		forceWrite:                     common.EOverwriteOption.True().String(),
		preserveOwner:                  common.PreserveOwnerDefault,
		includeDirectoryStubs:          true,
	}

	if params.blockBlobTier != common.EBlockBlobTier.None() {
		rawArgs.blockBlobTier = params.blockBlobTier.String()
	}
	if params.pageBlobTier != common.EPageBlobTier.None() {
		rawArgs.pageBlobTier = params.pageBlobTier.String()
	}
	if params.metadata != "" {
		rawArgs.metadata = params.metadata
	}
	if params.blobTags != nil {
		rawArgs.blobTags = params.blobTags.ToString()
	}

	return rawArgs
}