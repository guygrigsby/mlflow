package mlflow

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func TestCreateRun(t *testing.T) {
	tags := []RunTag{{Key: "env", Value: "test"}}
	var gotIn any
	c := fakeClient(func(method, path string, in, out any) error {
		gotIn = in
		if method != http.MethodPost {
			t.Errorf("method = %s, want POST", method)
		}
		if path != "runs/create" {
			t.Errorf("path = %s, want runs/create", path)
		}
		resp := map[string]any{
			"run": map[string]any{
				"info": map[string]any{"run_id": "r1", "experiment_id": "exp1"},
			},
		}
		b, _ := json.Marshal(resp)
		return json.Unmarshal(b, out)
	})

	run, err := c.CreateRun(context.Background(), "exp1",
		WithRunName("my-run"),
		WithStartTime(1234567890000),
		WithRunTags(tags...),
	)
	if err != nil {
		t.Fatal(err)
	}
	if run.Info.RunID != "r1" {
		t.Errorf("RunID = %s, want r1", run.Info.RunID)
	}

	rc, ok := gotIn.(*runCreate)
	if !ok {
		t.Fatalf("in type = %T, want *runCreate", gotIn)
	}
	if rc.ExperimentID != "exp1" {
		t.Errorf("ExperimentID = %s, want exp1", rc.ExperimentID)
	}
	if rc.RunName != "my-run" {
		t.Errorf("RunName = %s, want my-run", rc.RunName)
	}
	if rc.StartTime != 1234567890000 {
		t.Errorf("StartTime = %d, want 1234567890000", rc.StartTime)
	}
	if len(rc.Tags) != 1 || rc.Tags[0].Key != "env" || rc.Tags[0].Value != "test" {
		t.Errorf("Tags = %v, want [{env test}]", rc.Tags)
	}
}

func TestGetRun(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodGet {
			t.Errorf("method = %s, want GET", method)
		}
		// url.QueryEscape encodes "/" as "%2F"
		if path != "runs/get?run_id=abc%2F123" {
			t.Errorf("path = %s, want runs/get?run_id=abc%%2F123", path)
		}
		resp := map[string]any{
			"run": map[string]any{
				"info": map[string]any{"run_id": "abc/123"},
			},
		}
		b, _ := json.Marshal(resp)
		return json.Unmarshal(b, out)
	})

	run, err := c.GetRun(context.Background(), "abc/123")
	if err != nil {
		t.Fatal(err)
	}
	if run.Info.RunID != "abc/123" {
		t.Errorf("RunID = %s, want abc/123", run.Info.RunID)
	}
}

func TestUpdateRun_EndTimePresent(t *testing.T) {
	var gotIn any
	c := fakeClient(func(method, path string, in, out any) error {
		gotIn = in
		if method != http.MethodPost {
			t.Errorf("method = %s, want POST", method)
		}
		if path != "runs/update" {
			t.Errorf("path = %s, want runs/update", path)
		}
		return nil
	})

	if err := c.UpdateRun(context.Background(), "r1", StatusFinished, 9999); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(gotIn)
	var m map[string]any
	json.Unmarshal(b, &m)
	if _, ok := m["end_time"]; !ok {
		t.Error("end_time missing from request when nonzero")
	}
}

func TestUpdateRun_EndTimeAbsent(t *testing.T) {
	var gotIn any
	c := fakeClient(func(method, path string, in, out any) error {
		gotIn = in
		return nil
	})

	if err := c.UpdateRun(context.Background(), "r1", StatusRunning, 0); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(gotIn)
	var m map[string]any
	json.Unmarshal(b, &m)
	if _, ok := m["end_time"]; ok {
		t.Error("end_time present in request when zero, want omitted")
	}
}

func TestDeleteRun(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %s, want POST", method)
		}
		if path != "runs/delete" {
			t.Errorf("path = %s, want runs/delete", path)
		}
		b, _ := json.Marshal(in)
		var m map[string]any
		json.Unmarshal(b, &m)
		if m["run_id"] != "r1" {
			t.Errorf("run_id = %v, want r1", m["run_id"])
		}
		return nil
	})
	if err := c.DeleteRun(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreRun(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %s, want POST", method)
		}
		if path != "runs/restore" {
			t.Errorf("path = %s, want runs/restore", path)
		}
		b, _ := json.Marshal(in)
		var m map[string]any
		json.Unmarshal(b, &m)
		if m["run_id"] != "r2" {
			t.Errorf("run_id = %v, want r2", m["run_id"])
		}
		return nil
	})
	if err := c.RestoreRun(context.Background(), "r2"); err != nil {
		t.Fatal(err)
	}
}

func TestSetRunTag(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %s, want POST", method)
		}
		if path != "runs/set-tag" {
			t.Errorf("path = %s, want runs/set-tag", path)
		}
		b, _ := json.Marshal(in)
		var m map[string]any
		json.Unmarshal(b, &m)
		if m["run_id"] != "r3" {
			t.Errorf("run_id = %v, want r3", m["run_id"])
		}
		if m["key"] != "foo" {
			t.Errorf("key = %v, want foo", m["key"])
		}
		if m["value"] != "bar" {
			t.Errorf("value = %v, want bar", m["value"])
		}
		return nil
	})
	if err := c.SetRunTag(context.Background(), "r3", "foo", "bar"); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteRunTag(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %s, want POST", method)
		}
		if path != "runs/delete-tag" {
			t.Errorf("path = %s, want runs/delete-tag", path)
		}
		b, _ := json.Marshal(in)
		var m map[string]any
		json.Unmarshal(b, &m)
		if m["run_id"] != "r4" {
			t.Errorf("run_id = %v, want r4", m["run_id"])
		}
		if m["key"] != "baz" {
			t.Errorf("key = %v, want baz", m["key"])
		}
		return nil
	})
	if err := c.DeleteRunTag(context.Background(), "r4", "baz"); err != nil {
		t.Fatal(err)
	}
}

func TestSearchRuns(t *testing.T) {
	c := fakeClient(func(method, path string, in, out any) error {
		if method != http.MethodPost {
			t.Errorf("method = %s, want POST", method)
		}
		if path != "runs/search" {
			t.Errorf("path = %s, want runs/search", path)
		}
		resp := map[string]any{
			"runs": []map[string]any{
				{"info": map[string]any{"run_id": "r1"}},
				{"info": map[string]any{"run_id": "r2"}},
			},
			"next_page_token": "tok42",
		}
		b, _ := json.Marshal(resp)
		return json.Unmarshal(b, out)
	})

	runs, token, err := c.SearchRuns(context.Background(), SearchRunsRequest{
		ExperimentIDs: []string{"exp1"},
		MaxResults:    10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Errorf("len(runs) = %d, want 2", len(runs))
	}
	if runs[0].Info.RunID != "r1" {
		t.Errorf("runs[0].RunID = %s, want r1", runs[0].Info.RunID)
	}
	if token != "tok42" {
		t.Errorf("next_page_token = %s, want tok42", token)
	}
}
