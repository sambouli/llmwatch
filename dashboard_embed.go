package main

import (
	"embed"
	_ "embed"
)

// dashboardHTML is the single-page UI, served at /.
//
//go:embed dashboard.html
var dashboardHTML embed.FS

func init() {
	// unwrap the embedded FS to raw bytes
	b, err := dashboardHTML.ReadFile("dashboard.html")
	if err != nil {
		panic(err)
	}
	dashboardBytes = b
}

var dashboardBytes []byte
