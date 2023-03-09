PHONY: db, clippy, diesel, stop, tests, ignored, doc

DATABASE_URL := postgres://postgres:password@localhost/backie

db:
	docker run --rm -d --name backie-db -p 5432:5432 \
	  -e POSTGRES_DB=backie \
	  -e POSTGRES_USER=postgres \
	  -e POSTGRES_PASSWORD=password \
	  postgres:latest

clippy:
	cargo clippy --tests -- -D clippy::all

diesel:
	DATABASE_URL=$(DATABASE_URL) diesel migration run

stop:
	docker kill backie-db

tests:
	DATABASE_URL=$(DATABASE_URL) cargo test --all-features -- --color always --nocapture --test-threads 1

doc:
	cargo doc --open
