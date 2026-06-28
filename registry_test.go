package mlflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// splitPath separates a path from its query string.
func splitPath(path string) (base string, q url.Values) {
	idx := strings.IndexByte(path, '?')
	if idx == -1 {
		return path, url.Values{}
	}
	parsed, _ := url.ParseQuery(path[idx+1:])
	return path[:idx], parsed
}

// toMap marshals v to JSON then back to map[string]any so tests can inspect
// request fields without knowing the anonymous struct type.
func toMap(v any) map[string]any {
	b, _ := json.Marshal(v)
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}

// respond marshals payload as JSON and unmarshals it into out (simulates server).
func respond(out any, payload string) error {
	if out == nil {
		return nil
	}
	return json.Unmarshal([]byte(payload), out)
}

// --- Registered Model tests ---

func TestCreateRegisteredModel(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %q, want POST", method)
		}
		if path != "registered-models/create" {
			t.Errorf("path = %q, want registered-models/create", path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" {
			t.Errorf("name = %v", m["name"])
		}
		tags, _ := m["tags"].([]any)
		if len(tags) != 1 {
			t.Fatalf("tags len = %d, want 1", len(tags))
		}
		tag := tags[0].(map[string]any)
		if tag["key"] != "team" || tag["value"] != "ml" {
			t.Errorf("tag = %v", tag)
		}
		return respond(out, `{"registered_model":{"name":"mymodel","creation_timestamp":1000}}`)
	})
	m, err := c.CreateRegisteredModel(context.Background(), "mymodel", RegisteredModelTag{Key: "team", Value: "ml"})
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "mymodel" || m.CreationTimestamp != 1000 {
		t.Errorf("model = %+v", m)
	}
}

func TestGetRegisteredModel(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodGet {
			t.Errorf("method = %q, want GET", method)
		}
		base, q := splitPath(path)
		if base != "registered-models/get" {
			t.Errorf("base = %q", base)
		}
		if q.Get("name") != "mymodel" {
			t.Errorf("name param = %q", q.Get("name"))
		}
		if in != nil {
			t.Errorf("GET should have nil body, got %v", in)
		}
		return respond(out, `{"registered_model":{"name":"mymodel"}}`)
	})
	m, err := c.GetRegisteredModel(context.Background(), "mymodel")
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "mymodel" {
		t.Errorf("name = %q", m.Name)
	}
}

func TestRenameRegisteredModel(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %q, want POST", method)
		}
		if path != "registered-models/rename" {
			t.Errorf("path = %q", path)
		}
		m := toMap(in)
		if m["name"] != "old" || m["new_name"] != "new" {
			t.Errorf("body = %v", m)
		}
		return respond(out, `{"registered_model":{"name":"new"}}`)
	})
	m, err := c.RenameRegisteredModel(context.Background(), "old", "new")
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "new" {
		t.Errorf("name = %q", m.Name)
	}
}

func TestUpdateRegisteredModel(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %q, want POST", method)
		}
		if path != "registered-models/update" {
			t.Errorf("path = %q", path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" || m["description"] != "desc" {
			t.Errorf("body = %v", m)
		}
		return respond(out, `{"registered_model":{"name":"mymodel","description":"desc"}}`)
	})
	m, err := c.UpdateRegisteredModel(context.Background(), "mymodel", "desc")
	if err != nil {
		t.Fatal(err)
	}
	if m.Description != "desc" {
		t.Errorf("description = %q", m.Description)
	}
}

func TestDeleteRegisteredModel(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %q, want POST", method)
		}
		if path != "registered-models/delete" {
			t.Errorf("path = %q", path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" {
			t.Errorf("name = %v", m["name"])
		}
		return nil
	})
	if err := c.DeleteRegisteredModel(context.Background(), "mymodel"); err != nil {
		t.Fatal(err)
	}
}

func TestSearchRegisteredModels(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodGet {
			t.Errorf("method = %q, want GET", method)
		}
		base, q := splitPath(path)
		if base != "registered-models/search" {
			t.Errorf("base = %q", base)
		}
		if q.Get("filter") != "name LIKE 'my%'" {
			t.Errorf("filter = %q", q.Get("filter"))
		}
		if q.Get("max_results") != "10" {
			t.Errorf("max_results = %q", q.Get("max_results"))
		}
		if q.Get("page_token") != "tok1" {
			t.Errorf("page_token = %q", q.Get("page_token"))
		}
		return respond(out, `{"registered_models":[{"name":"mymodel"}],"next_page_token":"tok2"}`)
	})
	models, next, err := c.SearchRegisteredModels(context.Background(), "name LIKE 'my%'", 10, nil, "tok1")
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0].Name != "mymodel" {
		t.Errorf("models = %v", models)
	}
	if next != "tok2" {
		t.Errorf("next_page_token = %q", next)
	}
}

func TestSetRegisteredModelTag(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost || path != "registered-models/set-tag" {
			t.Errorf("method=%q path=%q", method, path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" || m["key"] != "k" || m["value"] != "v" {
			t.Errorf("body = %v", m)
		}
		return nil
	})
	if err := c.SetRegisteredModelTag(context.Background(), "mymodel", "k", "v"); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteRegisteredModelTag(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost || path != "registered-models/delete-tag" {
			t.Errorf("method=%q path=%q", method, path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" || m["key"] != "k" {
			t.Errorf("body = %v", m)
		}
		return nil
	})
	if err := c.DeleteRegisteredModelTag(context.Background(), "mymodel", "k"); err != nil {
		t.Fatal(err)
	}
}

func TestGetLatestVersions(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost || path != "registered-models/get-latest-versions" {
			t.Errorf("method=%q path=%q", method, path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" {
			t.Errorf("name = %v", m["name"])
		}
		stages, _ := m["stages"].([]any)
		if len(stages) != 1 || stages[0] != "Production" {
			t.Errorf("stages = %v", stages)
		}
		return respond(out, `{"model_versions":[{"name":"mymodel","version":"3","current_stage":"Production"}]}`)
	})
	versions, err := c.GetLatestVersions(context.Background(), "mymodel", "Production")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 || versions[0].Version != "3" {
		t.Errorf("versions = %v", versions)
	}
}

// --- Model Version tests ---

func TestCreateModelVersion(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost || path != "model-versions/create" {
			t.Errorf("method=%q path=%q", method, path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" || m["source"] != "s3://bucket/path" || m["run_id"] != "run1" {
			t.Errorf("body = %v", m)
		}
		tags, _ := m["tags"].([]any)
		if len(tags) != 1 {
			t.Errorf("tags = %v", tags)
		}
		return respond(out, `{"model_version":{"name":"mymodel","version":"1"}}`)
	})
	mv, err := c.CreateModelVersion(context.Background(), "mymodel", "s3://bucket/path", "run1", ModelVersionTag{Key: "k", Value: "v"})
	if err != nil {
		t.Fatal(err)
	}
	if mv.Name != "mymodel" || mv.Version != "1" {
		t.Errorf("model_version = %+v", mv)
	}
}

func TestGetModelVersion(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodGet {
			t.Errorf("method = %q, want GET", method)
		}
		base, q := splitPath(path)
		if base != "model-versions/get" {
			t.Errorf("base = %q", base)
		}
		if q.Get("name") != "mymodel" || q.Get("version") != "2" {
			t.Errorf("query = name=%q version=%q", q.Get("name"), q.Get("version"))
		}
		return respond(out, `{"model_version":{"name":"mymodel","version":"2"}}`)
	})
	mv, err := c.GetModelVersion(context.Background(), "mymodel", "2")
	if err != nil {
		t.Fatal(err)
	}
	if mv.Version != "2" {
		t.Errorf("version = %q", mv.Version)
	}
}

func TestUpdateModelVersion(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost || path != "model-versions/update" {
			t.Errorf("method=%q path=%q", method, path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" || m["version"] != "2" || m["description"] != "v2 desc" {
			t.Errorf("body = %v", m)
		}
		return respond(out, `{"model_version":{"name":"mymodel","version":"2","description":"v2 desc"}}`)
	})
	mv, err := c.UpdateModelVersion(context.Background(), "mymodel", "2", "v2 desc")
	if err != nil {
		t.Fatal(err)
	}
	if mv.Description != "v2 desc" {
		t.Errorf("description = %q", mv.Description)
	}
}

func TestDeleteModelVersion(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost || path != "model-versions/delete" {
			t.Errorf("method=%q path=%q", method, path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" || m["version"] != "2" {
			t.Errorf("body = %v", m)
		}
		return nil
	})
	if err := c.DeleteModelVersion(context.Background(), "mymodel", "2"); err != nil {
		t.Fatal(err)
	}
}

func TestSearchModelVersions(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodGet {
			t.Errorf("method = %q, want GET", method)
		}
		base, q := splitPath(path)
		if base != "model-versions/search" {
			t.Errorf("base = %q", base)
		}
		if q.Get("filter") != "name='mymodel'" {
			t.Errorf("filter = %q", q.Get("filter"))
		}
		if q.Get("page_token") != "pt" {
			t.Errorf("page_token = %q", q.Get("page_token"))
		}
		return respond(out, `{"model_versions":[{"name":"mymodel","version":"1"},{"name":"mymodel","version":"2"}],"next_page_token":"next"}`)
	})
	versions, next, err := c.SearchModelVersions(context.Background(), "name='mymodel'", 0, nil, "pt")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 2 {
		t.Fatalf("versions len = %d, want 2", len(versions))
	}
	if versions[0].Version != "1" || versions[1].Version != "2" {
		t.Errorf("versions = %v", versions)
	}
	if next != "next" {
		t.Errorf("next_page_token = %q", next)
	}
}

func TestSetModelVersionTag(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost || path != "model-versions/set-tag" {
			t.Errorf("method=%q path=%q", method, path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" || m["version"] != "1" || m["key"] != "k" || m["value"] != "v" {
			t.Errorf("body = %v", m)
		}
		return nil
	})
	if err := c.SetModelVersionTag(context.Background(), "mymodel", "1", "k", "v"); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteModelVersionTag(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost || path != "model-versions/delete-tag" {
			t.Errorf("method=%q path=%q", method, path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" || m["version"] != "1" || m["key"] != "k" {
			t.Errorf("body = %v", m)
		}
		return nil
	})
	if err := c.DeleteModelVersionTag(context.Background(), "mymodel", "1", "k"); err != nil {
		t.Fatal(err)
	}
}

func TestTransitionModelVersionStage(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost || path != "model-versions/transition-stage" {
			t.Errorf("method=%q path=%q", method, path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" {
			t.Errorf("name = %v", m["name"])
		}
		if m["version"] != "3" {
			t.Errorf("version = %v", m["version"])
		}
		if m["stage"] != "Production" {
			t.Errorf("stage = %v", m["stage"])
		}
		// archive_existing_versions must be present and true
		arch, ok := m["archive_existing_versions"].(bool)
		if !ok || !arch {
			t.Errorf("archive_existing_versions = %v (ok=%v)", m["archive_existing_versions"], ok)
		}
		return respond(out, `{"model_version":{"name":"mymodel","version":"3","current_stage":"Production"}}`)
	})
	mv, err := c.TransitionModelVersionStage(context.Background(), "mymodel", "3", "Production", true)
	if err != nil {
		t.Fatal(err)
	}
	if mv.CurrentStage != "Production" {
		t.Errorf("current_stage = %q", mv.CurrentStage)
	}
}

func TestGetModelVersionDownloadURI(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodGet {
			t.Errorf("method = %q, want GET", method)
		}
		base, q := splitPath(path)
		if base != "model-versions/get-download-uri" {
			t.Errorf("base = %q", base)
		}
		if q.Get("name") != "mymodel" || q.Get("version") != "1" {
			t.Errorf("query = name=%q version=%q", q.Get("name"), q.Get("version"))
		}
		return respond(out, `{"artifact_uri":"s3://bucket/path"}`)
	})
	uri, err := c.GetModelVersionDownloadURI(context.Background(), "mymodel", "1")
	if err != nil {
		t.Fatal(err)
	}
	if uri != "s3://bucket/path" {
		t.Errorf("uri = %q", uri)
	}
}

// --- Alias tests ---

func TestSetRegisteredModelAlias(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost || path != "registered-models/alias" {
			t.Errorf("method=%q path=%q", method, path)
		}
		m := toMap(in)
		if m["name"] != "mymodel" || m["alias"] != "champion" || m["version"] != "5" {
			t.Errorf("body = %v", m)
		}
		return nil
	})
	if err := c.SetRegisteredModelAlias(context.Background(), "mymodel", "champion", "5"); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteRegisteredModelAlias(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		// Must use DELETE, not POST
		if method != http.MethodDelete {
			t.Errorf("method = %q, want DELETE", method)
		}
		base, q := splitPath(path)
		if base != "registered-models/alias" {
			t.Errorf("base = %q", base)
		}
		if q.Get("name") != "mymodel" || q.Get("alias") != "champion" {
			t.Errorf("query = name=%q alias=%q", q.Get("name"), q.Get("alias"))
		}
		// Body must be nil for DELETE
		if in != nil {
			t.Errorf("DELETE body should be nil, got %v", in)
		}
		return nil
	})
	if err := c.DeleteRegisteredModelAlias(context.Background(), "mymodel", "champion"); err != nil {
		t.Fatal(err)
	}
}

func TestGetModelVersionByAlias(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodGet {
			t.Errorf("method = %q, want GET", method)
		}
		base, q := splitPath(path)
		if base != "registered-models/alias" {
			t.Errorf("base = %q", base)
		}
		if q.Get("name") != "mymodel" || q.Get("alias") != "champion" {
			t.Errorf("query = name=%q alias=%q", q.Get("name"), q.Get("alias"))
		}
		if in != nil {
			t.Errorf("GET body should be nil, got %v", in)
		}
		return respond(out, `{"model_version":{"name":"mymodel","version":"5","current_stage":"Production"}}`)
	})
	mv, err := c.GetModelVersionByAlias(context.Background(), "mymodel", "champion")
	if err != nil {
		t.Fatal(err)
	}
	if mv.Version != "5" || mv.CurrentStage != "Production" {
		t.Errorf("model_version = %+v", mv)
	}
}
