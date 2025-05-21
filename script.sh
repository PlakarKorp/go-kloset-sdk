#!/bin/bash
git add .

git commit -m "Update sdk"

git push

id=$(git rev-parse HEAD)

echo "The latest commit ID is: $id"

cd /home/peralban/Plakar/plakar

go get "github.com/PlakarKorp/go-kloset-sdk@$id"

go mod tidy

go mod vendor
