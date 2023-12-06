package config

var Settings SettingsTemplate

func init() {
	Settings.BadgerOptions.ValueLogFileSize = 2 << 30
	Settings.BadgerOptions.SyncWrites = true
}

type SettingsTemplate struct {
	BadgerOptions struct {
		ValueLogFileSize int64
		SyncWrites       bool
	}
}
