package mlflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
)

// injectJSON marshals v and decodes it into out, simulating what the real
// transport does when it decodes a 2xx response body.
func injectJSON(out, v any) {
	b, _ := json.Marshal(v)
	_ = json.Unmarshal(b, out)
}

func TestCreateExperiment(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %q, want POST", method)
		}
		if path != "experiments/create" {
			t.Errorf("path = %q, want experiments/create", path)
		}
		b, _ := json.Marshal(in)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if m["name"] != "myexp" {
			t.Errorf("name = %v, want myexp", m["name"])
		}
		tags, _ := m["tags"].([]any)
		if len(tags) != 1 {
			t.Errorf("tags len = %d, want 1", len(tags))
		}
		injectJSON(out, map[string]any{"experiment_id": "42"})
		return nil
	})
	id, err := c.CreateExperiment(context.Background(), "myexp", ExperimentTag{Key: "env", Value: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if id != "42" {
		t.Errorf("id = %q, want 42", id)
	}
}

func TestGetExperimentByName(t *testing.T) {
	name := "my experiment"
	wantPath := "experiments/get-by-name?experiment_name=" + url.QueryEscape(name)
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodGet {
			t.Errorf("method = %q, want GET", method)
		}
		if path != wantPath {
			t.Errorf("path = %q, want %q", path, wantPath)
		}
		if in != nil {
			t.Errorf("in = %v, want nil (GET must send no body)", in)
		}
		injectJSON(out, map[string]any{
			"experiment": map[string]any{"experiment_id": "7", "name": name},
		})
		return nil
	})
	exp, err := c.GetExperimentByName(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if exp.ExperimentID != "7" {
		t.Errorf("id = %q, want 7", exp.ExperimentID)
	}
	if exp.Name != name {
		t.Errorf("name = %q, want %q", exp.Name, name)
	}
}

func TestGetOrCreateExperiment_NotFound(t *testing.T) {
	calls := 0
	c := fakeClient(func(method, path string, in, out any) error {
		calls++
		if calls == 1 {
			// GetExperimentByName — not found
			return &APIError{Code: "RESOURCE_DOES_NOT_EXIST", HTTPStatus: 404, Message: "not found"}
		}
		// CreateExperiment
		injectJSON(out, map[string]any{"experiment_id": "9"})
		return nil
	})
	id, err := c.GetOrCreateExperiment(context.Background(), "newexp")
	if err != nil {
		t.Fatal(err)
	}
	if id != "9" {
		t.Errorf("id = %q, want 9", id)
	}
	if calls != 2 {
		t.Errorf("calls = %d, want 2 (get + create)", calls)
	}
}

func TestGetOrCreateExperiment_Exists(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		injectJSON(out, map[string]any{
			"experiment": map[string]any{"experiment_id": "3", "name": "existing"},
		})
		return nil
	})
	id, err := c.GetOrCreateExperiment(context.Background(), "existing")
	if err != nil {
		t.Fatal(err)
	}
	if id != "3" {
		t.Errorf("id = %q, want 3", id)
	}
}

func TestUpdateExperiment(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %q, want POST", method)
		}
		if path != "experiments/update" {
			t.Errorf("path = %q, want experiments/update", path)
		}
		b, _ := json.Marshal(in)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if m["experiment_id"] != "5" {
			t.Errorf("experiment_id = %v, want 5", m["experiment_id"])
		}
		if m["new_name"] != "renamed" {
			t.Errorf("new_name = %v, want renamed", m["new_name"])
		}
		return nil
	})
	if err := c.UpdateExperiment(context.Background(), "5", "renamed"); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteExperiment(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %q, want POST", method)
		}
		if path != "experiments/delete" {
			t.Errorf("path = %q, want experiments/delete", path)
		}
		b, _ := json.Marshal(in)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if m["experiment_id"] != "6" {
			t.Errorf("experiment_id = %v, want 6", m["experiment_id"])
		}
		return nil
	})
	if err := c.DeleteExperiment(context.Background(), "6"); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreExperiment(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %q, want POST", method)
		}
		if path != "experiments/restore" {
			t.Errorf("path = %q, want experiments/restore", path)
		}
		b, _ := json.Marshal(in)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if m["experiment_id"] != "6" {
			t.Errorf("experiment_id = %v, want 6", m["experiment_id"])
		}
		return nil
	})
	if err := c.RestoreExperiment(context.Background(), "6"); err != nil {
		t.Fatal(err)
	}
}

func TestSetExperimentTag(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %q, want POST", method)
		}
		if path != "experiments/set-experiment-tag" {
			t.Errorf("path = %q, want experiments/set-experiment-tag", path)
		}
		b, _ := json.Marshal(in)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if m["experiment_id"] != "2" {
			t.Errorf("experiment_id = %v, want 2", m["experiment_id"])
		}
		if m["key"] != "env" {
			t.Errorf("key = %v, want env", m["key"])
		}
		if m["value"] != "prod" {
			t.Errorf("value = %v, want prod", m["value"])
		}
		return nil
	})
	if err := c.SetExperimentTag(context.Background(), "2", "env", "prod"); err != nil {
		t.Fatal(err)
	}
}

func TestSearchExperiments(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %q, want POST", method)
		}
		if path != "experiments/search" {
			t.Errorf("path = %q, want experiments/search", path)
		}
		b, _ := json.Marshal(in)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if m["filter"] != "name LIKE '%test%'" {
			t.Errorf("filter = %v, want \"name LIKE '%%test%%'\"", m["filter"])
		}
		injectJSON(out, map[string]any{
			"experiments": []map[string]any{
				{"experiment_id": "1", "name": "test1"},
				{"experiment_id": "2", "name": "test2"},
			},
			"next_page_token": "tok123",
		})
		return nil
	})
	exps, tok, err := c.SearchExperiments(context.Background(), SearchExperimentsRequest{
		Filter:     "name LIKE '%test%'",
		MaxResults: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(exps) != 2 {
		t.Errorf("experiments len = %d, want 2", len(exps))
	}
	if tok != "tok123" {
		t.Errorf("next_page_token = %q, want tok123", tok)
	}
	if exps[0].ExperimentID != "1" || exps[1].ExperimentID != "2" {
		t.Errorf("ids = %q, %q; want 1, 2", exps[0].ExperimentID, exps[1].ExperimentID)
	}
}
