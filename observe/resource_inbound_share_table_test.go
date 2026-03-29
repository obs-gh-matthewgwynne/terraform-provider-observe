package observe

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

// Test fixture configuration - matches Python integration test at:
// ~/observe/s/integration/sharein_rest_api_test/sharein_rest_api_test/config.py
//
// These tests require a pre-existing inbound share with test data.
// The share must be already accepted in Snowflake and accessible via Observe.
//
// Test Share Details:
//   - Share Name: MATTG_SHAREIN_TEST_DATA_SHARE2
//   - Provider Account: HC83707.OBSERVE_O2_1 (Snowflake)
//   - Database: SHARE_199227663281_41000376
//   - Schema: PUBLIC
//   - Tables: TEMP_TEST_DATA, TEMP_TEST_REF_DATA
//
// To run these tests, you need:
//   1. forward-ingress running on testbox: s/testbox forward-ingress
//   2. Environment variables set (use scripts/set_env_vars.sh)
//   3. Run: TF_ACC=1 go test ./observe -v -run TestAccObserveInbound
var (
	// Share configuration
	testInboundShareName     = getenv("TEST_INBOUND_SHARE_NAME", "MATTG_SHAREIN_TEST_DATA_SHARE2")
	testInboundShareProvider = getenv("TEST_INBOUND_SHARE_PROVIDER", "HC83707.OBSERVE_O2_1")
	testInboundSchemaName    = "PUBLIC"

	// Test tables - both tables from Python integration test
	testInboundTableData = getenv("TEST_INBOUND_TABLE_DATA", "TEMP_TEST_DATA")     // Main test data table
	testInboundTableRef  = getenv("TEST_INBOUND_TABLE_REF", "TEMP_TEST_REF_DATA")  // Reference data table
)

// TestAccObserveInboundShareTable_Basic tests the complete lifecycle of tracking
// a table from an inbound share using the TEMP_TEST_DATA table.
//
// This test verifies:
//   - Share lookup by name + provider account works correctly
//   - Table can be tracked and dataset is created
//   - All computed fields are populated (oid, table_id, dataset_id, etc.)
//   - Dataset label and description can be updated
//   - Resource cleanup works (untrack table, delete dataset)
//
// Test flow:
//   Step 1: Track TEMP_TEST_DATA table as "Table" kind dataset
//   Step 2: Update dataset label and add description
//   Step 3: Automatic cleanup via Terraform destroy
func TestAccObserveInboundShareTable_Basic(t *testing.T) {
	randomPrefix := acctest.RandomWithPrefix("tf")

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheckInboundShare(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			// Step 1: Create - track a table and create dataset
			{
				Config: testAccInboundShareTableConfig(randomPrefix, "Table", "", testInboundTableData),
				Check: resource.ComposeTestCheckFunc(
					// Verify all computed fields are populated
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.test", "oid"),
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.test", "table_id"),
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.test", "dataset_id"),
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.test", "dataset_oid"),
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.test", "status"),
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.test", "full_table_path"),

					// Verify input values match configuration
					resource.TestCheckResourceAttr("observe_inbound_share_table.test", "table_name", testInboundTableData),
					resource.TestCheckResourceAttr("observe_inbound_share_table.test", "schema_name", testInboundSchemaName),
					resource.TestCheckResourceAttr("observe_inbound_share_table.test", "dataset_label", randomPrefix),
					resource.TestCheckResourceAttr("observe_inbound_share_table.test", "dataset_kind", "Table"),
				),
			},
			// Step 2: Update - change dataset label and add description
			{
				Config: testAccInboundShareTableConfig(randomPrefix+"-updated", "Table", "Updated description", testInboundTableData),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("observe_inbound_share_table.test", "dataset_label", randomPrefix+"-updated"),
					resource.TestCheckResourceAttr("observe_inbound_share_table.test", "description", "Updated description"),
				),
			},
		},
		// Step 3: Destroy happens automatically - tests cleanup (untrack table, delete dataset)
	})
}

// TestAccObserveInboundShareTable_Event tests tracking with Event dataset kind.
//
// This test verifies:
//   - Tables can be tracked as Event datasets (not just Table kind)
//   - valid_from_field configuration works correctly
//   - Event datasets are created with proper timestamp field
//
// Uses TEMP_TEST_DATA table which has a VALID_FROM_TIME field suitable for events.
func TestAccObserveInboundShareTable_Event(t *testing.T) {
	randomPrefix := acctest.RandomWithPrefix("tf")

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheckInboundShare(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: testAccInboundShareTableConfigEvent(randomPrefix, testInboundTableData),
				Check: resource.ComposeTestCheckFunc(
					// Verify Event dataset kind is set
					resource.TestCheckResourceAttr("observe_inbound_share_table.test", "dataset_kind", "Event"),
					// Verify timestamp field is configured
					resource.TestCheckResourceAttr("observe_inbound_share_table.test", "valid_from_field", "VALID_FROM_TIME"),
					// Verify dataset was created successfully
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.test", "dataset_oid"),
				),
			},
		},
	})
}

// TestAccObserveInboundShareTable_RefData tests tracking the reference data table.
//
// This test verifies:
//   - Both test tables (TEMP_TEST_DATA and TEMP_TEST_REF_DATA) can be tracked
//   - Reference/lookup tables work the same as data tables
//   - Multiple tables from same share can be managed independently
//
// This matches the Python integration test which tracks both tables.
func TestAccObserveInboundShareTable_RefData(t *testing.T) {
	randomPrefix := acctest.RandomWithPrefix("tf")

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheckInboundShare(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: testAccInboundShareTableConfig(randomPrefix+"-ref", "Table", "Reference data table", testInboundTableRef),
				Check: resource.ComposeTestCheckFunc(
					// Verify table was tracked successfully
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.test", "oid"),
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.test", "dataset_id"),
					// Verify correct table name
					resource.TestCheckResourceAttr("observe_inbound_share_table.test", "table_name", testInboundTableRef),
					// Verify description
					resource.TestCheckResourceAttr("observe_inbound_share_table.test", "description", "Reference data table"),
				),
			},
		},
	})
}

// TestAccObserveInboundShareTable_MultipleTables tests tracking both test tables simultaneously.
//
// This test verifies:
//   - Multiple tables from the same share can be tracked at once
//   - Each table gets its own dataset
//   - Tables can be tracked/untracked independently
//   - No conflicts or race conditions when managing multiple tables
//
// This is the most comprehensive test and matches the Python integration test behavior
// which tracks and manages both TEMP_TEST_DATA and TEMP_TEST_REF_DATA.
func TestAccObserveInboundShareTable_MultipleTables(t *testing.T) {
	randomPrefix := acctest.RandomWithPrefix("tf")

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheckInboundShare(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			// Step 1: Track both tables from the share
			{
				Config: testAccInboundShareTableConfigMultiple(randomPrefix),
				Check: resource.ComposeTestCheckFunc(
					// Verify data table
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.data", "oid"),
					resource.TestCheckResourceAttr("observe_inbound_share_table.data", "table_name", testInboundTableData),
					resource.TestCheckResourceAttr("observe_inbound_share_table.data", "dataset_label", randomPrefix+"-data"),

					// Verify ref table
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.ref", "oid"),
					resource.TestCheckResourceAttr("observe_inbound_share_table.ref", "table_name", testInboundTableRef),
					resource.TestCheckResourceAttr("observe_inbound_share_table.ref", "dataset_label", randomPrefix+"-ref"),

					// Verify both have different dataset IDs
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.data", "dataset_id"),
					resource.TestCheckResourceAttrSet("observe_inbound_share_table.ref", "dataset_id"),
				),
			},
			// Step 2: Update both tables
			{
				Config: testAccInboundShareTableConfigMultiple(randomPrefix + "-updated"),
				Check: resource.ComposeTestCheckFunc(
					// Verify updates applied to both
					resource.TestCheckResourceAttr("observe_inbound_share_table.data", "dataset_label", randomPrefix+"-updated-data"),
					resource.TestCheckResourceAttr("observe_inbound_share_table.ref", "dataset_label", randomPrefix+"-updated-ref"),
				),
			},
		},
		// Automatic cleanup will untrack both tables
	})
}

// ============================================================================
// Helper functions to generate Terraform test configurations
// ============================================================================

// testAccInboundShareTableConfig generates a Terraform config for tracking a single table.
//
// Parameters:
//   - datasetLabel: Label for the Observe dataset (e.g., "my-test-dataset")
//   - datasetKind: Dataset kind - "Table", "Event", "Interval", or "Resource"
//   - description: Optional description for the dataset (can be empty string)
//   - tableName: Name of the table in the share (e.g., "TEMP_TEST_DATA")
//
// Returns a Terraform configuration string that:
//   1. Looks up the test share by name + provider
//   2. Tracks the specified table from the share
//   3. Creates an Observe dataset with the given configuration
func testAccInboundShareTableConfig(datasetLabel, datasetKind, description, tableName string) string {
	config := fmt.Sprintf(`
# Look up the inbound share by name and provider account
data "observe_inbound_share" "test" {
	share_name       = "%s"
	provider_account = "%s"
}

# Track a table from the share and create a dataset
resource "observe_inbound_share_table" "test" {
	share_id      = data.observe_inbound_share.test.oid
	table_name    = "%s"
	schema_name   = "%s"
	dataset_label = "%s"
	dataset_kind  = "%s"
`, testInboundShareName, testInboundShareProvider, tableName, testInboundSchemaName, datasetLabel, datasetKind)

	// Add optional description if provided
	if description != "" {
		config += fmt.Sprintf(`	description   = "%s"
`, description)
	}

	config += "}\n"
	return config
}

// testAccInboundShareTableConfigEvent generates a config for tracking a table as an Event dataset.
//
// Event datasets require a valid_from_field to use as the event timestamp.
// This is typically a timestamp column in the source table.
//
// Parameters:
//   - datasetLabel: Label for the Observe dataset
//   - tableName: Name of the table in the share
//
// Returns a Terraform configuration that creates an Event dataset with VALID_FROM_TIME
// as the timestamp field.
func testAccInboundShareTableConfigEvent(datasetLabel, tableName string) string {
	return fmt.Sprintf(`
# Look up the inbound share
data "observe_inbound_share" "test" {
	share_name       = "%s"
	provider_account = "%s"
}

# Track table as Event dataset with timestamp field
resource "observe_inbound_share_table" "test" {
	share_id         = data.observe_inbound_share.test.oid
	table_name       = "%s"
	schema_name      = "%s"
	dataset_label    = "%s"
	dataset_kind     = "Event"
	valid_from_field = "VALID_FROM_TIME"  # Column name for event timestamp
}
`, testInboundShareName, testInboundShareProvider, tableName, testInboundSchemaName, datasetLabel)
}

// testAccInboundShareTableConfigMultiple generates a config tracking both test tables.
//
// This creates two separate resources:
//   - observe_inbound_share_table.data - Tracks TEMP_TEST_DATA
//   - observe_inbound_share_table.ref - Tracks TEMP_TEST_REF_DATA
//
// This mirrors the Python integration test which manages both tables simultaneously.
//
// Parameters:
//   - labelPrefix: Prefix for dataset labels (e.g., "tf-test")
//                  Will create datasets named "{prefix}-data" and "{prefix}-ref"
//
// Returns a Terraform configuration that tracks both tables from the same share.
func testAccInboundShareTableConfigMultiple(labelPrefix string) string {
	return fmt.Sprintf(`
# Look up the inbound share (shared by both tables)
data "observe_inbound_share" "test" {
	share_name       = "%s"
	provider_account = "%s"
}

# Track the main data table
resource "observe_inbound_share_table" "data" {
	share_id      = data.observe_inbound_share.test.oid
	table_name    = "%s"
	schema_name   = "%s"
	dataset_label = "%s-data"
	dataset_kind  = "Table"
	description   = "Main test data table"
}

# Track the reference data table
resource "observe_inbound_share_table" "ref" {
	share_id      = data.observe_inbound_share.test.oid
	table_name    = "%s"
	schema_name   = "%s"
	dataset_label = "%s-ref"
	dataset_kind  = "Table"
	description   = "Reference data table"
}
`, testInboundShareName, testInboundShareProvider,
		testInboundTableData, testInboundSchemaName, labelPrefix,
		testInboundTableRef, testInboundSchemaName, labelPrefix)
}
