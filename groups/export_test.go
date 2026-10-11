package groups

// SetSyncBudget sets how large a Sync grows before the next one starts, and
// returns a function that sets it back.
func SetSyncBudget(n int) func() {
	old := syncBudget
	syncBudget = n
	return func() { syncBudget = old }
}
