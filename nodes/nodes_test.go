package nodes

import (
   "context"
   "os"
   "path/filepath"
   "reflect"
   "net/http"
   "net/http/httptest"
   "testing"
   "data-pipeline/helpers"
)

func TestAggregateExampleNode(t *testing.T) {
   cfg := map[string]interface{}{"groupByField": "UserID"}
   node := NewAggregateExampleNode("agg", cfg)
   items := []interface{}{ 
       map[string]interface{}{"UserID": "a"},
       map[string]interface{}{"UserID": "b"},
       map[string]interface{}{"UserID": "a"},
       map[string]interface{}{"UserID": "c"},
       map[string]interface{}{"Other": "x"},
   }
   out, err := node.Process(context.Background(), items)
   if err != nil {
       t.Fatalf("Process error: %v", err)
   }
   results := make(map[string]int)
   for _, o := range out {
       m, ok := o.(map[string]interface{})
       if !ok {
           t.Fatalf("expected map[string]interface{}, got %T", o)
       }
       key, _ := m["UserID"].(string)
       count, _ := m["count"].(int)
       results[key] = count
   }
   expected := map[string]int{"a": 2, "b": 1, "c": 1}
   if !reflect.DeepEqual(results, expected) {
       t.Errorf("unexpected aggregation, got %v, want %v", results, expected)
   }
}

func TestExportContactsNode(t *testing.T) {
   cfg := map[string]interface{}{"endpoint": "http://example.com", "apiKey": "key"}
   node := NewExportContactsNode("export", cfg)
   items := []interface{}{1, "two", 3.0}
   out, err := node.Process(context.Background(), items)
   if err != nil {
       t.Fatalf("Process error: %v", err)
   }
   if !reflect.DeepEqual(out, items) {
       t.Errorf("unexpected output, got %v, want %v", out, items)
   }
}

func TestImportAnalyticsNode(t *testing.T) {
   tmpFile, err := os.CreateTemp("", "events_*.log")
   if err != nil {
       t.Fatalf("CreateTemp error: %v", err)
   }
   defer os.Remove(tmpFile.Name())
   lines := []string{
       `{"event":"e1"}`,
       "",
       `{"event":"e2"}`,
       "invalidjson",
       `{"event":"e3"}`,
   }
   for _, l := range lines {
       tmpFile.WriteString(l + "\n")
   }
   tmpFile.Close()

   cfg := map[string]interface{}{"sourceFile": tmpFile.Name()}
   node := NewImportAnalyticsNode("importA", cfg)
   out, err := node.Process(context.Background(), nil)
   if err != nil {
       t.Fatalf("Process error: %v", err)
   }
   if len(out) != 3 {
       t.Errorf("expected 3 events, got %d", len(out))
   }
   for i, want := range []string{"e1", "e2", "e3"} {
       m, ok := out[i].(map[string]interface{})
       if !ok {
           t.Fatalf("expected map for event %d, got %T", i, out[i])
       }
       if m["event"] != want {
           t.Errorf("event %d: got %v, want %v", i, m["event"], want)
       }
   }
}

func TestImportContactsNode(t *testing.T) {
   tmpDir := t.TempDir()
   cachePath := filepath.Join(tmpDir, "cache.json")
   cfg := map[string]interface{}{"cacheFilePath": cachePath}
   node := NewImportContactsNode("importC", cfg)
   out, err := node.Process(context.Background(), nil)
   if err != nil {
       t.Fatalf("Process error: %v", err)
   }
   if len(out) != 3 {
       t.Errorf("expected 3 contacts, got %d", len(out))
   }
   fc, err := helpers.NewFileCache(cachePath)
   if err != nil {
       t.Fatalf("NewFileCache error: %v", err)
   }
   val, found := fc.Get("last_import_date")
   if !found || val == "" {
       t.Error("expected last_import_date in cache, got none")
   }
}

func TestMongoPersistNode(t *testing.T) {
   cfg := map[string]interface{}{"uri": "mongodb://localhost", "database": "db", "collection": "col"}
   node := NewMongoPersistNode("mongo", cfg)
   items := []interface{}{map[string]interface{}{"foo": "bar"}}
   out, err := node.Process(context.Background(), items)
   if err != nil {
       t.Fatalf("Process error: %v", err)
   }
   if !reflect.DeepEqual(out, items) {
       t.Errorf("unexpected output, got %v, want %v", out, items)
   }
}

func TestTransformNode(t *testing.T) {
   cfg := map[string]interface{}{"uppercaseField": "name"}
   node := NewTransformNode("trans", cfg)
   items := []interface{}{ 
       map[string]interface{}{"name": "alice", "id": 1},
       map[string]interface{}{"name": "Bob"},
       "not a map",
   }
   out, err := node.Process(context.Background(), items)
   if err != nil {
       t.Fatalf("Process error: %v", err)
   }
   if len(out) != 2 {
       t.Errorf("expected 2 output items, got %d", len(out))
   }
   m0 := out[0].(map[string]interface{})
   if m0["name"] != "ALICE" {
       t.Errorf("expected ALICE, got %v", m0["name"])
   }
   m1 := out[1].(map[string]interface{})
   if m1["name"] != "BOB" {
       t.Errorf("expected BOB, got %v", m1["name"])
   }
}
// Test successful fetching and parsing of HubSpot contacts using CRM v3 API.
func TestImportHubspotContactsNode(t *testing.T) {
    // Example response with one contact
    const exampleJSON = `{"paging":{"next":{"link":"?after=NTI1Cg%3D%3D","after":"NTI1Cg%3D%3D"}},"results":[{"createdAt":"2025-04-18T11:47:59.737Z","archived":false,"id":"string","objectWriteTraceId":"string","properties":{"email":"mark.s@lumon.industries","lastname":"S.","firstname":"Mark","createdate":"2025-02-11T15:45:00.400Z","hs_object_id":"98630638716","lastmodifieddate":"2025-02-11T15:52:10.273Z"},"updatedAt":"2025-04-18T11:47:59.737Z"}]}`
 
    // Mock HTTP server
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Verify headers and query parameters
        if auth := r.Header.Get("Authorization"); auth != "Bearer TOKEN" {
            t.Errorf("expected Authorization header 'Bearer TOKEN', got '%s'", auth)
        }
        if limit := r.URL.Query().Get("limit"); limit != "1" {
            t.Errorf("expected limit=1, got '%s'", limit)
        }
        if archived := r.URL.Query().Get("archived"); archived != "false" {
            t.Errorf("expected archived=false, got '%s'", archived)
        }
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(http.StatusOK)
        w.Write([]byte(exampleJSON))
    }))
    defer server.Close()
 
    // Use a temporary cache file
    cacheFile := filepath.Join(t.TempDir(), "cache.json")
    config := map[string]interface{}{ // limit as int type
        "apiKey":        "TOKEN",
        "endpoint":      server.URL,
        "limit":         1,
        "cacheFilePath": cacheFile,
    }
    node := NewImportHubspotContactsNode("testNode", config)
 
    items, err := node.Process(context.Background(), nil)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if len(items) != 1 {
        t.Fatalf("expected 1 item, got %d", len(items))
    }
    // Assert the returned result contains the expected email
    record, ok := items[0].(map[string]interface{})
    if !ok {
        t.Fatalf("expected record to be map[string]interface{}, got %T", items[0])
    }
    props, ok := record["properties"].(map[string]interface{})
    if !ok {
        t.Fatalf("expected properties map, got %T", record["properties"])
    }
    email, ok := props["email"].(string)
    if !ok {
        t.Fatalf("expected email to be string, got %T", props["email"])
    }
    if email != "mark.s@lumon.industries" {
        t.Errorf("expected email 'mark.s@lumon.industries', got '%s'", email)
    }
 }

