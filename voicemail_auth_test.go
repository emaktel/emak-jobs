package main

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Test key and fixtures shared with voicemail_storage_auth.lua and the SvelteKit verifier test
// (src/lib/server/auth/voicemailAuth.test.ts): the same inputs must give the same URL.
const testKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

var testNow = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

func TestPresignMatchesLuaSigner(t *testing.T) {
	cases := []struct{ method, url, want string }{
		{
			"POST",
			"https://alpha.emaktech.com/api/v0/voicemails/greetings/upload/clinic.emaktech.com/101/1?previous_filename=" + url.QueryEscape("greeting_1_x y+z.wav"),
			"https://alpha.emaktech.com/api/v0/voicemails/greetings/upload/clinic.emaktech.com/101/1?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=f1-dev%2F20261006%2Fvoicemail%2Fs3%2Faws4_request&X-Amz-Date=20261006T120000Z&X-Amz-Expires=120&X-Amz-SignedHeaders=host&previous_filename=greeting_1_x%20y%2Bz.wav&X-Amz-Signature=a0d38bed9ca40c4a755249aa2fc4ca84dd5b2c4d3cd631d5089ef36a4edbdd75",
		},
		{
			"PATCH",
			"https://alpha.emaktech.com/api/v0/voicemails/messages/transcription/3f1c2a9e-1111-4222-8333-444455556666",
			"https://alpha.emaktech.com/api/v0/voicemails/messages/transcription/3f1c2a9e-1111-4222-8333-444455556666?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=f1-dev%2F20261006%2Fvoicemail%2Fs3%2Faws4_request&X-Amz-Date=20261006T120000Z&X-Amz-Expires=120&X-Amz-SignedHeaders=host&X-Amz-Signature=adf97fde047ac959ea9b88841e5b0a7dbeae77e04a677ea017a4635ef1aa597e",
		},
	}
	for _, c := range cases {
		req, err := http.NewRequest(c.method, c.url, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := presignVoicemail(req, "f1-dev", testKey, testNow); err != nil {
			t.Fatal(err)
		}
		if got := req.URL.String(); got != c.want {
			t.Errorf("%s %s\n got %s\nwant %s", c.method, c.url, got, c.want)
		}
	}
}

func TestPresignRefusesPathOutsideAPI(t *testing.T) {
	req, _ := http.NewRequest("GET", "https://alpha.emaktech.com/other/x", nil)
	if err := presignVoicemail(req, "f1-dev", testKey, testNow); err == nil {
		t.Fatal("expected an error")
	}
}

func TestLoadVoicemailCredential(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.xml")
	os.WriteFile(good, []byte(`<include>
  <profile name="voicemail-storage">
    <aws-s3>
      <access-key-id><![CDATA[f1]]></access-key-id>
      <secret-access-key><![CDATA[`+testKey+`]]></secret-access-key>
      <region>voicemail</region>
    </aws-s3>
  </profile>
</include>`), 0600)
	id, secret, err := loadVoicemailCredential(good)
	if err != nil || id != "f1" || secret != testKey {
		t.Fatalf("got %q, secret ok=%v, err=%v", id, secret == testKey, err)
	}

	empty := filepath.Join(dir, "empty.xml")
	os.WriteFile(empty, []byte(`<include><profile name="voicemail-storage"><aws-s3></aws-s3></profile></include>`), 0600)
	if _, _, err := loadVoicemailCredential(empty); err == nil {
		t.Fatal("expected an error for a profile without a key")
	}
	if _, _, err := loadVoicemailCredential(filepath.Join(dir, "missing.xml")); err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

// The credential file may hold plain text or CDATA, like any http_cache profile; this parser and
// the Lua one (tests/voicemail_storage_auth_test.lua) must read every form alike.
func TestLoadVoicemailCredentialForms(t *testing.T) {
	profile := func(id, key string) string {
		return "<include>\n  <profile name=\"voicemail-storage\">\n    <aws-s3>\n      <access-key-id>" + id +
			"</access-key-id>\n      <secret-access-key>" + key + "</secret-access-key>\n      <region>voicemail</region>\n    </aws-s3>\n  </profile>\n</include>\n"
	}
	cases := []struct{ name, file, wantID, wantKey string }{
		{"CDATA", profile("<![CDATA[f1-dev]]>", "<![CDATA["+testKey+"]]>"), "f1-dev", testKey},
		{"plain", profile("f1-dev", testKey), "f1-dev", testKey},
		{"plain with surrounding whitespace", profile("\n        f1-dev\n      ", "  "+testKey+"\n"), "f1-dev", testKey},
		{"CDATA with whitespace around and inside", profile("  <![CDATA[f1-dev]]>\n", "\n<![CDATA[ "+testKey+" ]]> "), "f1-dev", testKey},
		{"plain id, CDATA key", profile("f1-dev", "<![CDATA["+testKey+"]]>"), "f1-dev", testKey},
		{"plain entities are decoded", profile("f1-dev", "k&amp;e&lt;y&gt;&quot;&apos;"), "f1-dev", "k&e<y>\"'"},
		{"&amp;lt; decodes once", profile("f1-dev", "a&amp;lt;b"), "f1-dev", "a&lt;b"},
		{"CDATA is not entity-decoded", profile("f1-dev", "<![CDATA[a&lt;b]]>"), "f1-dev", "a&lt;b"},
	}
	dir := t.TempDir()
	for i, c := range cases {
		path := filepath.Join(dir, fmt.Sprintf("cred%d.xml", i))
		os.WriteFile(path, []byte(c.file), 0600)
		id, key, err := loadVoicemailCredential(path)
		if err != nil || id != c.wantID || key != c.wantKey {
			t.Errorf("%s: got id %q, key match %v, err %v", c.name, id, key == c.wantKey, err)
		}
	}

	// Plain and CDATA files sign identically (the Lua signer's fixture).
	plain := filepath.Join(dir, "plain.xml")
	os.WriteFile(plain, []byte(profile("f1-dev", testKey)), 0600)
	id, key, err := loadVoicemailCredential(plain)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("PATCH", "https://alpha.emaktech.com/api/v0/voicemails/messages/transcription/3f1c2a9e-1111-4222-8333-444455556666", nil)
	if err := presignVoicemail(req, id, key, testNow); err != nil {
		t.Fatal(err)
	}
	if got := req.URL.Query().Get("X-Amz-Signature"); got != "adf97fde047ac959ea9b88841e5b0a7dbeae77e04a677ea017a4635ef1aa597e" {
		t.Errorf("plain-form credential signed %s", got)
	}

	blank := filepath.Join(dir, "blank.xml")
	os.WriteFile(blank, []byte(profile("f1-dev", "  ")), 0600)
	if _, _, err := loadVoicemailCredential(blank); err == nil {
		t.Error("expected an error for a blank key")
	}
}

func TestAuthForVmFetchSignsAndNeverSendsAuthorization(t *testing.T) {
	dir := t.TempDir()
	vmCredentialFile = filepath.Join(dir, "cred.xml")
	os.WriteFile(vmCredentialFile, []byte(`<include><profile name="voicemail-storage"><aws-s3><access-key-id><![CDATA[f2]]></access-key-id><secret-access-key><![CDATA[`+testKey+`]]></secret-access-key></aws-s3></profile></include>`), 0600)
	req, _ := http.NewRequest("DELETE", "https://alpha.emaktalk.com/api/v0/voicemails/messages/r2/clinic.emaktalk.com/101/3f1c2a9e-1111-4222-8333-444455556666", nil)
	if err := authFor("vm_fetch", req); err != nil {
		t.Fatal(err)
	}
	if req.Header.Get("Authorization") != "" || req.URL.User != nil {
		t.Fatal("vm_fetch must not send a credential")
	}
	q := req.URL.Query()
	if q.Get("X-Amz-Credential")[:3] != "f2/" || len(q.Get("X-Amz-Signature")) != 64 {
		t.Fatalf("unexpected query %s", req.URL.RawQuery)
	}
}
