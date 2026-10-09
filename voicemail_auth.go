package main

// Voicemail storage signing (Loi 5 U10). Jobs with auth "vm_fetch" call the SvelteKit voicemail
// storage routes; instead of the old platform-wide secret they carry an AWS Signature V4
// presigned query made with this box's key, the same form FreeSWITCH's mod_http_cache produces
// for playback. The scheme and its two fixed choices (UNSIGNED-PAYLOAD; signed host = request
// host + "/api/v0") are documented in fusion-svelte src/lib/server/auth/voicemailAuth.ts, which
// verifies it, and in scripts/resources/functions/voicemail_storage_auth.lua.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	vmSigningRegion  = "voicemail"
	vmSigningService = "s3"
	vmHostSuffix     = "/api/v0"
	vmExpiresS       = 120
)

// vmCredentialFile is the http_cache profile that http_cache.conf.xml includes; one file holds
// the box's caller id and key for all three signers.
var vmCredentialFile = envOr("EMAK_JOBS_VOICEMAIL_CREDENTIAL", "/etc/freeswitch/voicemail_storage_profile.xml")

type vmProfileFile struct {
	Profile struct {
		AWS struct {
			KeyID  string `xml:"access-key-id"`
			Secret string `xml:"secret-access-key"`
		} `xml:"aws-s3"`
	} `xml:"profile"`
}

// loadVoicemailCredential reads the file on every job, so a rotated key needs no restart. Values may
// be plain text or CDATA, trimmed, as mod_http_cache reads them.
func loadVoicemailCredential(path string) (id, secret string, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	var f vmProfileFile
	if err := xml.Unmarshal(b, &f); err != nil {
		return "", "", fmt.Errorf("%s: %w", path, err)
	}
	id, secret = strings.TrimSpace(f.Profile.AWS.KeyID), strings.TrimSpace(f.Profile.AWS.Secret)
	if id == "" || secret == "" {
		return "", "", fmt.Errorf("%s has no access-key-id or secret-access-key", path)
	}
	return id, secret, nil
}

// presignVoicemail adds the X-Amz-* query parameters, signature last, to req.URL.
func presignVoicemail(req *http.Request, id, secret string, now time.Time) error {
	path := req.URL.EscapedPath()
	if !strings.HasPrefix(path, vmHostSuffix+"/") {
		return fmt.Errorf("path outside %s: %s", vmHostSuffix, path)
	}
	if req.URL.Host == "" {
		return errors.New("url has no host")
	}
	amzDate := now.UTC().Format("20060102T150405Z")
	dateStamp := amzDate[:8]
	scope := dateStamp + "/" + vmSigningRegion + "/" + vmSigningService + "/aws4_request"

	q := req.URL.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", id+"/"+scope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", fmt.Sprint(vmExpiresS))
	q.Set("X-Amz-SignedHeaders", "host")
	q.Del("X-Amz-Signature")
	canonicalQuery := vmCanonicalQuery(q)

	canonicalRequest := strings.Join([]string{
		req.Method, path, canonicalQuery, "host:" + req.URL.Host + vmHostSuffix, "", "host", "UNSIGNED-PAYLOAD",
	}, "\n")
	digest := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, hex.EncodeToString(digest[:])}, "\n")

	key := vmHMAC([]byte("AWS4"+secret), dateStamp)
	for _, part := range []string{vmSigningRegion, vmSigningService, "aws4_request"} {
		key = vmHMAC(key, part)
	}
	signature := hex.EncodeToString(vmHMAC(key, stringToSign))
	req.URL.RawQuery = canonicalQuery + "&X-Amz-Signature=" + signature
	return nil
}

// vmCanonicalQuery: every parameter SigV4-encoded, sorted by name then value.
func vmCanonicalQuery(q url.Values) string {
	var pairs [][2]string
	for name, values := range q {
		for _, v := range values {
			pairs = append(pairs, [2]string{vmURIEncode(name), vmURIEncode(v)})
		}
	}
	sort.Slice(pairs, func(a, b int) bool {
		if pairs[a][0] == pairs[b][0] {
			return pairs[a][1] < pairs[b][1]
		}
		return pairs[a][0] < pairs[b][0]
	})
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p[0] + "=" + p[1]
	}
	return strings.Join(out, "&")
}

// vmURIEncode: everything but A-Z a-z 0-9 - _ . ~ as uppercase %XX.
func vmURIEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func vmHMAC(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}
