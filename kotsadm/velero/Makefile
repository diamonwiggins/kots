SHELL := /bin/bash
PROJECT_NAME ?= velero
PLUGIN_NAME ?= velero-plugin-for-aws

.PHONY: publish
publish: VELERO_IMAGE = kotsadm/${PROJECT_NAME}:${IMAGE_TAG}
publish: PLUGIN_IMAGE = kotsadm/${PLUGIN_NAME}:${IMAGE_TAG}
publish:
	docker build -f Dockerfile.velero -t ${VELERO_IMAGE} .
	docker push ${VELERO_IMAGE}
	docker build -f Dockerfile.velero-plugin-for-aws -t ${PLUGIN_IMAGE} .
	docker push ${PLUGIN_IMAGE}

.PHONY: publish-latest
publish-latest: IMAGE_TAG = alpha
publish-latest: publish

.PHONY: publish-release
publish-release: IMAGE_TAG = ${GIT_TAG}
publish-release: publish
	mkdir -p bin/docker-archive/${PROJECT_NAME}
	skopeo copy docker-daemon:kotsadm/${PROJECT_NAME}:${GIT_TAG} docker-archive:bin/docker-archive/${PROJECT_NAME}/${GIT_TAG}
	mkdir -p bin/docker-archive/${PLUGIN_NAME}
	skopeo copy docker-daemon:kotsadm/${PLUGIN_NAME}:${GIT_TAG} docker-archive:bin/docker-archive/${PLUGIN_NAME}/${GIT_TAG}
