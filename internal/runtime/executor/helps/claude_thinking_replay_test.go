package helps

import (
	"testing"
)

// observedFable5CAIS is the Claude CAIS envelope fixture from
// internal/signature (observed claude-fable-5 thinking signature).
const observedFable5CAIS = "CAISqwIKiAEIEBgCKkBHRlRBsNiptQUWfPoOhuQKwi5LnncZVO9bB5jqOs76D7uBtgktML0zqJtNmLHXHHcgD6lk4MQu4QBXzFd1lbC3Mg5jbGF1ZGUtZmFibGUtNTgBQgh0aGlua2luZ1okZDk3NDM5NzUtNGJiMC00OTM2LTllMjgtZDViMGQyMWJkYzQ4EgxCGh+XVFFFeySAjtAaDL/A1LltGu6MMJ+eXSIwsN0oBpDrqLv22UBfkMnTotnIbkvkOyb9xZHgigG6OZVHaI3gThm+maLKmgO5PrFLKlDFYp+YZksy/wKwszJlnLTPzAK+NUlfzagOE1ymtZTXhAYK260XyFYmg/te/C231+Fr/hoX+EJoUBnrn0gD7hqMISOT+TaFEuOXYsN517GfaxgB"

func TestClaudeThinkingReplayContentIsReplayable_AcceptsCAISSignedTurn(t *testing.T) {
	content := []byte(`[{"type":"thinking","thinking":"provider reasoning","signature":"` + observedFable5CAIS + `"},{"type":"text","text":"answer"}]`)
	if !ClaudeThinkingReplayContentIsReplayable(content) {
		t.Fatal("CAIS-signed thinking turn must be replayable so the cache can store it")
	}
}
