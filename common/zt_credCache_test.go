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

package common

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"testing"
	"time"

	chk "gopkg.in/check.v1"
)

// Hookup to the testing framework
func Test(t *testing.T) { chk.TestingT(t) }

type credCacheTestSuite struct{}

var _ = chk.Suite(&credCacheTestSuite{})

var fakeTokenInfo = OAuthTokenInfo{
	AccessToken: azcore.AccessToken{
		Token:     "aaa0eXAiOiJKz1QiLCJhbGciOibbbbI1NiIsIng1dCcCImlCakwxUmNdddhpeTRmcHhJeGRacW5oTTJZayIsImtpZCI948lCakwxUmNxemhpeTRmcHhJeGRacW9oTTJZayJ9.eyJhdWQiOiJodHRwczovL3N0b3JhZ2UuYXp1cmUuY29tIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvNzJmOTg4YmYtODZmMS00MWFmLTkxYWItMmQ3Y2QwMTFkYjQ3LyIsImlhdCI6MTUyODEwNDQ5NywibmJmIjoxNTI4MTA0NDk3LCJleHAiOjE1MjgxMDgzOTcsIl9jbGFpbV9uYW1lcyI6eyJncm91aEHiOiJzcmMxIn0sIl9jbGFpbV9zb3VyY2VzIjp7InNyYzEiOnsiZW5kcG9pbnQiOiJodHRwczovL2dyYXBoLndpbmRvd3MubmV0LzcyZjk4OGJmLTg2ZjEtNDFhZi05MWFiLTJkN2NkMDExZGI0Ny91c2Vycy9hOTIzZjhkMC1kNGNlLTQyODAtOTEzNS01ZWE2ODVjMzgwMjYvZ2V0TWVtYmVyT2JqZWN0cyJ9fSwiYWNyIjoiMSIsImFpbyI6IkFVUUF1LzhIQUFBQU1nVkUzWE9DMHdQcG9OeGt1b2VsK1haVGNwOEhLekRORlp4NDZkMW5VN2VHUGNmbWdWNGxnUlN0NjUwcndXaHJPaCtaTXlGa3h2S3hVR3QvTHBjanNnPT0iLCJhbXIiOlsid2lhIiwibWZhIl0sImFwcGlkIjoiMTk1MGEyNTgtMjI3Yi00ZTMxLWE5Y2YtNzE3NDk1OTQ1ZmMyIiwiYXBwaWRhY3IiOiIwIiwiZGV2aWNlaWQiOiIyMjFjZTY3Yy1mYjM3LTQzMjYtYWJjYy0zNTRhZGJmNzk1NWYiLCJmYW1pbHlfbmFtZSI6IkZhbiIsImdpdmVuX25hbWUiOiJKYXNvbiIsImluX2NvcnAiOiJ0cnVlIiwiaXBhZGRyIjoiMTY3LjIyMC4yNTUuNTgiLCJuYW1lIjoiSmFzb24gRmFuIiwib2lkIjoiYTkyM2Y4ZDAtZDRjZS00MjgwLTkxMzUtNWVhNjg1YzM4MDI2Iiwib25wcmVtX3NpZCI6IlMtMS01LTIxLTIxNDY3NzMwODUtOTAzMzYzMjg1LTcxOTM0NDcwNy0xODI4ODgzIiwicHVpZCI6IjEwMDMwMDAwOEFCNjkzQTUi10JzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzdWIiOiJBVVBFWXo1Y0xPd1BYcmRQaUF2OXZRamNGelpDN3dRRWd5dUJhejFfVnBFIiwidGlkIjoiNzJmOTg4YmYtODZmMS00MWFmLTkxYWItMmQ3Y2QwMTFkYjQ3IiwidW5pcXVlX25hbWUiOiJqaWFjZmFuQG1pY3Jvc29mdC5jb20iLCJ1cG4iOiJqaWFjZmFuQG1pY3Jvc29mdC5jb20iLCJ1dGkiOiJfTlpKdlVQVG4wdTExTVFrTEcwTEFBIiwidmVyIjoiMS4wIn0.J3LZgQ7RTmqZzVcnsiruzLfcuK-vceNja7gp6wJhwwcPN1LzHK9Q1ANRVBKDMRulHiWvPNmavxf493EqkvgjHDkGSSTL3S7elLVF4Hr2SHHhUqyWoiEukY0jX5DT2tg71L4KujV7csJN-7ECqXyU0DSrRSRf3gCbD7c2ne5CFVCi1lEpEK_1lLiRZe45TTuJXmQrxEr4B6fY5MRkBz05lIbhxsUPmUunR02_-coNgQcHBOkdGdLGx4qjbzn58EJO0F2bimDRend3Tjnoia2aFq_kvQslcLU3BxIvYO5TZNfGkZyOlavoKEccPPmAb033zg9AKD_6_7K-R0mu1qmZUA",
		ExpiresOn: time.UnixMilli(1528108397).UTC(),
	},
	Resource:                "https://storage.azure.com/.default",
	Tenant:                  "Microsoft.com",
	ActiveDirectoryEndpoint: "https://login.microsoftonline.com",
}

func (s *credCacheTestSuite) TestCredCacheSaveLoadDeleteHas(c *chk.C) {
	credCache := NewCredCache(CredCacheOptions{
		DPAPIFilePath: ".",
		KeyName:       "AzCopyOAuthTokenCache",
		ServiceName:   "AzCopyV10",
		AccountName:   "AzCopyOAuthTokenCache",
	})

	defer func() {
		// Cleanup fake token
		hasCachedToken, _ := credCache.HasCachedToken()
		if hasCachedToken {
			_ = credCache.RemoveCachedToken()
		}
	}()

	// Prepare and clean cache for testing.
	hasCachedToken, err := credCache.HasCachedToken()
	if hasCachedToken {
		err = credCache.RemoveCachedToken()
		c.Assert(err, chk.IsNil)
	}

	// Ensure no token cached initially.
	hasCachedToken, err = credCache.HasCachedToken()
	c.Assert(hasCachedToken, chk.Equals, false)

	// Test save token.
	err = credCache.SaveToken(fakeTokenInfo)
	c.Assert(err, chk.IsNil)

	// Test has cached token, and validate save token.
	hasCachedToken, err = credCache.HasCachedToken()
	c.Assert(err, chk.IsNil)
	c.Assert(hasCachedToken, chk.Equals, true)

	// Test load token.
	token, err := credCache.LoadToken()
	c.Assert(err, chk.IsNil)
	c.Assert(token, chk.NotNil)
	c.Assert(*token, chk.DeepEquals, fakeTokenInfo)

	// Test update token.
	cloneTokenWithDiff := fakeTokenInfo // deep copy
	cloneTokenWithDiff.Tenant = "change the tenant info a little"
	err = credCache.SaveToken(cloneTokenWithDiff)
	c.Assert(err, chk.IsNil)
	token, err = credCache.LoadToken()
	c.Assert(err, chk.IsNil)
	c.Assert(token, chk.NotNil)
	c.Assert(*token, chk.DeepEquals, cloneTokenWithDiff)

	// Test remove token.
	err = credCache.RemoveCachedToken()
	c.Assert(err, chk.IsNil)

	// Test has cached token, and validate remove token.
	hasCachedToken, err = credCache.HasCachedToken()
	c.Assert(hasCachedToken, chk.Equals, false)
}
