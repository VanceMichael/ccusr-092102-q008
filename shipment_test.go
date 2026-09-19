package shipment_test

import (
	"encoding/json"
	"os"
	"testing"
)

func TestShipmentFixtureKeepsLotAndBox(t *testing.T) {
	raw, err := os.ReadFile("fixtures/shipment.json")
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	if value["bonded_lot"] == "" || value["box_code"] == "" {
		t.Fatal("保税批次与箱码不能为空")
	}
}
