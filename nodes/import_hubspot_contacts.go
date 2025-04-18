package nodes

import (
   "context"
   "data-pipeline/helpers"
   "encoding/json"
   "fmt"
   "io"
   "log"
   "net/http"
   "net/url"
   "strconv"
   "time"
)

func init() {
   Register("importHubspotContacts", NewImportHubspotContactsNode)
}

// ImportHubspotContactsNode fetches HubSpot contacts that were modified since
// the last successful run.
//
// # Pipeline configuration example
//
// pipelines:
//   main_contact_flow:
//     - name: "ImportHubspotContacts"
//       type: "importHubspotContacts"
//       concurrency: 1
//       batchSize: 100
//       config:
//         apiKey: ${HUBSPOT_API_KEY}          // or set directly
//         limit: 100                          // optional, default 100
//         cacheFilePath: "./cache/hubspot_contacts_cache.json"  // optional
//
type ImportHubspotContactsNode struct {
   name      string
   config    map[string]interface{}
   cache     *helpers.FileCache
   cacheFile string
}

// NewImportHubspotContactsNode creates a new ImportHubspotContactsNode.
// It initializes a file cache to store the last time_offset.
func NewImportHubspotContactsNode(name string, config map[string]interface{}) *ImportHubspotContactsNode {
   var cacheFilePath string
   if path, ok := config["cacheFilePath"].(string); ok && path != "" {
       cacheFilePath = path
   } else {
       cacheFilePath = fmt.Sprintf("./cache/%s_cache.json", name)
   }
   cache, err := helpers.NewFileCache(cacheFilePath)
   if err != nil {
       log.Printf("Warning: could not initialize cache for node %s: %v", name, err)
       cache = nil
   }
   return &ImportHubspotContactsNode{
       name:      name,
       config:    config,
       cache:     cache,
       cacheFile: cacheFilePath,
   }
}

// Name returns the node's name.
func (n *ImportHubspotContactsNode) Name() string {
   return n.name
}

// Process fetches contacts from HubSpot API modified since the last run.
// The API key and optional endpoint/limit can be configured via the node's config.
func (n *ImportHubspotContactsNode) Process(ctx context.Context, items []interface{}) ([]interface{}, error) {
   // Retrieve API key
   apiKey, _ := n.config["apiKey"].(string)
   if apiKey == "" {
       return nil, fmt.Errorf("apiKey must be provided for node %s", n.name)
   }
   // Determine endpoint (HubSpot CRM v3 API)
   endpoint := "https://api.hubapi.com/crm/v3/objects/contacts"
   if v, ok := n.config["endpoint"].(string); ok && v != "" {
       endpoint = v
   }
   // Determine limit (count)
   limit := 100
   if v, ok := n.config["limit"].(float64); ok {
       limit = int(v)
   } else if v, ok := n.config["limit"].(int); ok {
       limit = v
   }
   // Retrieve last updated timestamp from cache
   var updatedAfter int64
   if n.cache != nil {
       if s, found := n.cache.Get("time_offset"); found {
           if ua, err := strconv.ParseInt(s, 10, 64); err == nil {
               updatedAfter = ua
           } else {
               log.Printf("[%s] Warning: invalid cached time_offset: %v", n.name, err)
           }
       }
   }
   // Record current time for next run
   runTime := time.Now().UnixNano() / int64(time.Millisecond)
   // Build request URL with HubSpot CRM v3 API
   params := url.Values{}
   params.Set("limit", strconv.Itoa(limit))
   if updatedAfter > 0 {
       params.Set("updatedAfter", strconv.FormatInt(updatedAfter, 10))
   }
   params.Set("archived", "false")
   reqURL := fmt.Sprintf("%s?%s", endpoint, params.Encode())
   log.Printf("[%s] Fetching HubSpot contacts from %s", n.Name(), reqURL)
   // Create HTTP request
   req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
   if err != nil {
       return nil, fmt.Errorf("failed to create request: %w", err)
   }
   req.Header.Set("Authorization", "Bearer "+apiKey)
   // Execute request
   resp, err := http.DefaultClient.Do(req)
   if err != nil {
       return nil, fmt.Errorf("request error: %w", err)
   }
   defer resp.Body.Close()
   if resp.StatusCode != http.StatusOK {
       bodyBytes, _ := io.ReadAll(resp.Body)
       return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
   }
   // Read and parse response
   body, err := io.ReadAll(resp.Body)
   if err != nil {
       return nil, fmt.Errorf("failed to read response body: %w", err)
   }
   var result struct {
       Results []map[string]interface{} `json:"results"`
       Paging  struct {
           Next struct {
               After string `json:"after"`
               Link  string `json:"link"`
           } `json:"next"`
       } `json:"paging"`
   }
   if err := json.Unmarshal(body, &result); err != nil {
       return nil, fmt.Errorf("failed to parse response JSON: %w", err)
   }
   // Update cache with new timestamp
   if n.cache != nil {
       if err := n.cache.Set("time_offset", strconv.FormatInt(runTime, 10)); err != nil {
           log.Printf("[%s] Warning: failed to update cache: %v", n.Name(), err)
       } else {
           log.Printf("[%s] Updated time_offset in cache to %d", n.Name(), runTime)
       }
   }
   // Convert results to []interface{}
   output := make([]interface{}, len(result.Results))
   for i, r := range result.Results {
       output[i] = r
   }
   return output, nil
}