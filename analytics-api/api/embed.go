// Package api embeds the OpenAPI document so the binary serves exactly the
// spec it was built with.
package api

import _ "embed"

//go:embed openapi.yaml
var OpenAPI []byte
