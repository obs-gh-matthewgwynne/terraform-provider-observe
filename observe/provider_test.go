package observe

import (
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

var testAccProviders map[string]*schema.Provider
var testAccProvider *schema.Provider

func init() {
	testAccProvider = Provider()
	testAccProviders = map[string]*schema.Provider{
		"observe": testAccProvider,
	}
}

func TestProvider(t *testing.T) {
	if err := Provider().InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func testAccPreCheck(t *testing.T) {
	requiredEnvVars := []string{"OBSERVE_CUSTOMER", "OBSERVE_DOMAIN"}

	for _, k := range requiredEnvVars {
		if v := os.Getenv(k); v == "" {
			t.Fatalf("%s must be set for acceptance tests", k)
		}
	}
}

// testAccPreCheckInboundShare verifies prerequisites for inbound share tests.
// Skips test with WARNING if SKIP_INBOUND_SHARE_TESTS=true.
func testAccPreCheckInboundShare(t *testing.T) {
	// Run standard prechecks first
	testAccPreCheck(t)

	// Check if inbound share tests should be skipped
	if os.Getenv("SKIP_INBOUND_SHARE_TESTS") == "true" {
		t.Log("")
		t.Log("╔═══════════════════════════════════════════════════════════════╗")
		t.Log("║  ⚠️  WARNING: Inbound Share Tests SKIPPED                    ║")
		t.Log("╚═══════════════════════════════════════════════════════════════╝")
		t.Log("")
		t.Log("These tests require a pre-configured inbound share:")
		t.Log("  Share Name:       " + getenv("TEST_INBOUND_SHARE_NAME", "MATTG_SHAREIN_TEST_DATA_SHARE2"))
		t.Log("  Provider Account: " + getenv("TEST_INBOUND_SHARE_PROVIDER", "HC83707.OBSERVE_O2_1"))
		t.Log("")
		t.Log("To enable these tests:")
		t.Log("  1. Ensure test share exists in your environment")
		t.Log("  2. Run: export SKIP_INBOUND_SHARE_TESTS=false")
		t.Log("  3. Or use: source scripts/set_env_vars.sh")
		t.Log("")
		t.Skip("Inbound share tests disabled via SKIP_INBOUND_SHARE_TESTS=true")
	}

	// Verify test fixture configuration
	testShare := getenv("TEST_INBOUND_SHARE_NAME", "")
	testProvider := getenv("TEST_INBOUND_SHARE_PROVIDER", "")

	if testShare == "" {
		t.Log("⚠️  WARNING: TEST_INBOUND_SHARE_NAME not set, using default")
	}
	if testProvider == "" {
		t.Log("⚠️  WARNING: TEST_INBOUND_SHARE_PROVIDER not set, using default")
	}
}
