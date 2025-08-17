#!/bin/bash

REGISTRY=docker.io/your-dockerhub-user
IMAGE=sallma-controller
TAG=$(git rev-parse --short HEAD)

docker build -t $REGISTRY/$IMAGE:$TAG -f deployment/docker/Dockerfile.controller .
echo $DOCKER_TOKEN | docker login -u $DOCKER_USER --password-stdin
docker push $REGISTRY/$IMAGE:$TAG
