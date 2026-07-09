module orbitcp

go 1.22

require github.com/lib/pq v1.10.9

// Vendored locally because this dev environment has no access to the Go
// module proxy. In a normal environment this would just be a plain
// `require` resolved from proxy.golang.org -- functionally identical
// either way, this only affects where the source comes from.
replace github.com/lib/pq => ./third_party/pq
