SHELL := /bin/bash
PROJECT_NAME ?= velero
AWS_PLUGIN_NAME ?= velero-plugin-for-aws
RESTIC_RESTORE_NAME ?= velero-restic-restore-helper

.PHONY: publish
publish: VELERO_IMAGE = kotsadm/${PROJECT_NAME}:${IMAGE_TAG}
publish: AWS_PLUGIN_IMAGE = kotsadm/${AWS_PLUGIN_NAME}:${IMAGE_TAG}
publish: RESTIC_RESTORE_IMAGE = kotsadm/${RESTIC_RESTORE_NAME}:${IMAGE_TAG}
publish:
	docker build -f Dockerfile.velero -t ${VELERO_IMAGE} .
	docker push ${VELERO_IMAGE}
	docker build -f Dockerfile.velero-aws -t ${AWS_PLUGIN_IMAGE} .
	docker push ${AWS_PLUGIN_IMAGE}
	docker build -f Dockerfile.restic-restore -t ${RESTIC_RESTORE_IMAGE} .
	docker push ${RESTIC_RESTORE_IMAGE}

.PHONY: publish-latest
publish-latest: IMAGE_TAG = alpha
publish-latest: publish

.PHONY: publish-release
publish-release: IMAGE_TAG = ${GIT_TAG}
publish-release: publish
	mkdir -p bin/docker-archive/${PROJECT_NAME}
	skopeo copy docker-daemon:kotsadm/${PROJECT_NAME}:${GIT_TAG} docker-archive:bin/docker-archive/${PROJECT_NAME}/${GIT_TAG}
	mkdir -p bin/docker-archive/${AWS_PLUGIN_NAME}
	skopeo copy docker-daemon:kotsadm/${AWS_PLUGIN_NAME}:${GIT_TAG} docker-archive:bin/docker-archive/${AWS_PLUGIN_NAME}/${GIT_TAG}
	mkdir -p bin/docker-archive/${RESTIC_RESTORE_NAME}
	skopeo copy docker-daemon:kotsadm/${RESTIC_RESTORE_NAME}:${GIT_TAG} docker-archive:bin/docker-archive/${RESTIC_RESTORE_NAME}/${GIT_TAG}
