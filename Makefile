

test:
	go test -v ./... -coverprofile=coverage.out -covermode=atomic -coverpkg=./...

coverage-html:
	go tool cover -html=coverage.out

# Lifecycle acceptance: drives real ffmpeg/ffprobe through a midway stop and a
# stop-then-restart, using the repository 3gp sample.
lifecycle-check:
	go run ./examples/lifecycle-check -input e2e/fixtures/input.3gp
