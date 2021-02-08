package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/replicatedhq/kots/kotsadm/pkg/k8s"
	"github.com/replicatedhq/kots/kotsadm/pkg/kurl"
	"github.com/replicatedhq/kots/kotsadm/pkg/logger"
	"github.com/replicatedhq/kots/kotsadm/pkg/snapshot"
	snapshottypes "github.com/replicatedhq/kots/kotsadm/pkg/snapshot/types"
	"github.com/replicatedhq/kots/kotsadm/pkg/store"
	"github.com/replicatedhq/kots/pkg/k8sutil"
	"github.com/replicatedhq/kots/pkg/kotsadm"
	kotssnapshot "github.com/replicatedhq/kots/pkg/snapshot"
	kotssnapshottypes "github.com/replicatedhq/kots/pkg/snapshot/types"
	"github.com/robfig/cron"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
)

type ConfigureNFSSnapshotsResponse struct {
	Success              bool   `json:"success"`
	Error                string `json:"error,omitempty"`
	IsMinimalRBACEnabled bool   `json:"isMinimalRBACEnabled,omitempty"`
}

type ConfigureNFSSnapshotsRequest struct {
	NFSOptions NFSOptions `json:"nfsOptions"`
}

type NFSOptions struct {
	Path       string `json:"path"`
	Server     string `json:"server"`
	ForceReset bool   `json:"forceReset,omitempty"`
}

type GlobalSnapshotSettingsResponse struct {
	VeleroVersion   string   `json:"veleroVersion"`
	VeleroPlugins   []string `json:"veleroPlugins"`
	IsVeleroRunning bool     `json:"isVeleroRunning"`
	ResticVersion   string   `json:"resticVersion"`
	IsResticRunning bool     `json:"isResticRunning"`
	IsKurl          bool     `json:"isKurl"`

	Store     *kotssnapshottypes.Store     `json:"store,omitempty"`
	NFSConfig *kotssnapshottypes.NFSConfig `json:"nfsConfig,omitempty"`

	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type UpdateGlobalSnapshotSettingsRequest struct {
	Provider string `json:"provider"`
	Bucket   string `json:"bucket"`
	Path     string `json:"path"`

	AWS      *kotssnapshottypes.StoreAWS    `json:"aws"`
	Google   *kotssnapshottypes.StoreGoogle `json:"gcp"`
	Azure    *kotssnapshottypes.StoreAzure  `json:"azure"`
	Other    *kotssnapshottypes.StoreOther  `json:"other"`
	Internal bool                           `json:"internal"`
	NFS      *NFSOptions                    `json:"nfs"`
}

type SnapshotConfig struct {
	AutoEnabled  bool                            `json:"autoEnabled"`
	AutoSchedule *snapshottypes.SnapshotSchedule `json:"autoSchedule"`
	TTl          *snapshottypes.SnapshotTTL      `json:"ttl"`
}

type VeleroStatus struct {
	IsVeleroInstalled bool `json:"isVeleroInstalled"`
}

func (h *Handler) ConfigureNFSSnapshots(w http.ResponseWriter, r *http.Request) {
	response := ConfigureNFSSnapshotsResponse{
		Success: false,
	}

	request := ConfigureNFSSnapshotsRequest{}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		errMsg := "failed to decode request body"
		logger.Error(errors.Wrap(err, errMsg))
		response.Error = errMsg
		JSON(w, http.StatusBadRequest, response)
		return
	}

	clientset, err := k8s.Clientset()
	if err != nil {
		errMsg := "failed to get k8s client set"
		response.Error = errMsg
		logger.Error(errors.Wrap(err, errMsg))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// check if minimal rbac is enabled, if so, kotsadm won't have sufficient permissions
	// to install velero, in this case, nfs & velero have to be installed/configured using the CLI

	namespace := os.Getenv("POD_NAMESPACE")

	isMinimalRBACEnabled := !k8sutil.IsKotsadmClusterScoped(r.Context(), clientset)
	if isMinimalRBACEnabled {
		response.IsMinimalRBACEnabled = true
		JSON(w, http.StatusOK, response)
		return
	}

	// deploy/configure nfs minio

	if err := configureNFSMinio(r.Context(), clientset, &request.NFSOptions); err != nil {
		if _, ok := errors.Cause(err).(*kotssnapshot.ResetNFSError); ok {
			response.Error = err.Error()
			JSON(w, http.StatusConflict, response)
			return
		}
		errMsg := "failed to configure nfs minio"
		response.Error = errMsg
		logger.Error(errors.Wrap(err, errMsg))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// install/configure velero

	registryOptions, err := kotsadm.GetKotsadmOptionsFromCluster(namespace, clientset)
	if err != nil {
		errMsg := "failed to get kotsadm options from cluster"
		response.Error = errMsg
		logger.Error(errors.Wrap(err, errMsg))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	veleroNamespace, err := kotssnapshot.DetectVeleroNamespace()
	if err != nil {
		errMsg := "failed to detect velero namespace"
		response.Error = errMsg
		logger.Error(errors.Wrap(err, errMsg))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if veleroNamespace == "" {
		// velero not found, install and configure velero using NFS store
		if err := kotssnapshot.InstallVeleroFromStoreNFS(r.Context(), clientset, namespace, registryOptions, true); err != nil {
			errMsg := "failed to install velero"
			response.Error = errMsg
			logger.Error(errors.Wrap(err, errMsg))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		response.Success = true
		response.IsMinimalRBACEnabled = false

		JSON(w, http.StatusOK, response)
		return
	}

	// velero is already installed, only configure velero images and the store

	err = kotssnapshot.ConfigureVeleroImages(r.Context(), clientset, namespace, registryOptions)
	if err != nil {
		errMsg := "failed to configure velero images"
		response.Error = errMsg
		logger.Error(errors.Wrap(err, errMsg))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	configureStoreOptions := kotssnapshot.ConfigureStoreOptions{
		NFS:              true,
		KotsadmNamespace: namespace,
	}
	_, err = kotssnapshot.ConfigureStore(configureStoreOptions)
	if err != nil {
		errMsg := "failed to configure store"
		response.Error = errMsg
		logger.Error(errors.Wrap(err, errMsg))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = kotssnapshot.WaitForVeleroReady(r.Context(), clientset, nil)
	if err != nil {
		errMsg := "failed to wait for velero"
		response.Error = errMsg
		logger.Error(errors.Wrap(err, errMsg))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	response.Success = true
	response.IsMinimalRBACEnabled = false

	JSON(w, http.StatusOK, response)
}

func configureNFSMinio(ctx context.Context, clientset kubernetes.Interface, opts *NFSOptions) error {
	namespace := os.Getenv("POD_NAMESPACE")

	registryOptions, err := kotsadm.GetKotsadmOptionsFromCluster(namespace, clientset)
	if err != nil {
		return errors.Wrap(err, "failed to get kotsadm options from cluster")
	}

	deployOptions := kotssnapshot.NFSDeployOptions{
		Namespace:   namespace,
		IsOpenShift: k8sutil.IsOpenShift(clientset),
		ForceReset:  opts.ForceReset,
		NFSConfig: kotssnapshottypes.NFSConfig{
			Path:   opts.Path,
			Server: opts.Server,
		},
		Wait: true,
	}
	if err := kotssnapshot.DeployNFSMinio(ctx, clientset, deployOptions, registryOptions); err != nil {
		return err
	}

	return nil
}

func (h *Handler) UpdateGlobalSnapshotSettings(w http.ResponseWriter, r *http.Request) {
	globalSnapshotSettingsResponse := GlobalSnapshotSettingsResponse{
		Success: false,
	}

	updateGlobalSnapshotSettingsRequest := UpdateGlobalSnapshotSettingsRequest{}
	if err := json.NewDecoder(r.Body).Decode(&updateGlobalSnapshotSettingsRequest); err != nil {
		logger.Error(err)
		globalSnapshotSettingsResponse.Error = "failed to decode request body"
		JSON(w, http.StatusBadRequest, globalSnapshotSettingsResponse)
		return
	}

	veleroStatus, err := kotssnapshot.DetectVelero()
	if err != nil {
		logger.Error(err)
		globalSnapshotSettingsResponse.Error = "failed to detect velero"
		JSON(w, http.StatusInternalServerError, globalSnapshotSettingsResponse)
		return
	}
	if veleroStatus == nil {
		JSON(w, http.StatusOK, globalSnapshotSettingsResponse)
		return
	}

	globalSnapshotSettingsResponse.VeleroVersion = veleroStatus.Version
	globalSnapshotSettingsResponse.VeleroPlugins = veleroStatus.Plugins
	globalSnapshotSettingsResponse.IsVeleroRunning = veleroStatus.Status == "Ready"
	globalSnapshotSettingsResponse.ResticVersion = veleroStatus.ResticVersion
	globalSnapshotSettingsResponse.IsResticRunning = veleroStatus.ResticStatus == "Ready"
	globalSnapshotSettingsResponse.IsKurl = kurl.IsKurl()

	if updateGlobalSnapshotSettingsRequest.NFS != nil {
		// make sure NFS Minio is configured and deployed first
		clientset, err := k8s.Clientset()
		if err != nil {
			err = errors.Wrap(err, "failed to get k8s client set")
			logger.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := configureNFSMinio(r.Context(), clientset, updateGlobalSnapshotSettingsRequest.NFS); err != nil {
			if _, ok := errors.Cause(err).(*kotssnapshot.ResetNFSError); ok {
				globalSnapshotSettingsResponse.Error = err.Error()
				JSON(w, http.StatusConflict, globalSnapshotSettingsResponse)
				return
			}
			err = errors.Wrap(err, "failed to configure nfs minio")
			logger.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	// update/configure store
	options := kotssnapshot.ConfigureStoreOptions{
		Provider: updateGlobalSnapshotSettingsRequest.Provider,
		Bucket:   updateGlobalSnapshotSettingsRequest.Bucket,
		Path:     updateGlobalSnapshotSettingsRequest.Path,

		AWS:      updateGlobalSnapshotSettingsRequest.AWS,
		Google:   updateGlobalSnapshotSettingsRequest.Google,
		Azure:    updateGlobalSnapshotSettingsRequest.Azure,
		Other:    updateGlobalSnapshotSettingsRequest.Other,
		Internal: updateGlobalSnapshotSettingsRequest.Internal,
		NFS:      updateGlobalSnapshotSettingsRequest.NFS != nil,

		KotsadmNamespace: os.Getenv("POD_NAMESPACE"),
	}
	updatedStore, err := kotssnapshot.ConfigureStore(options)
	if err != nil {
		if _, ok := errors.Cause(err).(*kotssnapshot.InvalidStoreDataError); ok {
			logger.Error(err)
			JSON(w, http.StatusBadRequest, NewErrorResponse(err))
			return
		}
		err = errors.Wrap(err, "failed to configure snapshots")
		logger.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if updatedStore.NFS != nil {
		nfsConfig, err := kotssnapshot.GetCurrentNFSConfig(r.Context(), os.Getenv("POD_NAMESPACE"))
		if err != nil {
			err = errors.Wrap(err, "failed to get nfs config")
			logger.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		globalSnapshotSettingsResponse.NFSConfig = nfsConfig
	}

	globalSnapshotSettingsResponse.Store = updatedStore
	globalSnapshotSettingsResponse.Success = true

	JSON(w, http.StatusOK, globalSnapshotSettingsResponse)
}

func (h *Handler) GetGlobalSnapshotSettings(w http.ResponseWriter, r *http.Request) {
	globalSnapshotSettingsResponse := GlobalSnapshotSettingsResponse{
		Success: false,
	}

	veleroStatus, err := kotssnapshot.DetectVelero()
	if err != nil {
		logger.Error(err)
		globalSnapshotSettingsResponse.Error = "failed to detect velero"
		JSON(w, http.StatusInternalServerError, globalSnapshotSettingsResponse)
		return
	}
	if veleroStatus == nil {
		JSON(w, http.StatusOK, globalSnapshotSettingsResponse)
		return
	}

	globalSnapshotSettingsResponse.VeleroVersion = veleroStatus.Version
	globalSnapshotSettingsResponse.VeleroPlugins = veleroStatus.Plugins
	globalSnapshotSettingsResponse.IsVeleroRunning = veleroStatus.Status == "Ready"
	globalSnapshotSettingsResponse.ResticVersion = veleroStatus.ResticVersion
	globalSnapshotSettingsResponse.IsResticRunning = veleroStatus.ResticStatus == "Ready"
	globalSnapshotSettingsResponse.IsKurl = kurl.IsKurl()

	store, err := kotssnapshot.GetGlobalStore(nil)
	if err != nil {
		logger.Error(err)
		globalSnapshotSettingsResponse.Error = "failed to get store"
		JSON(w, http.StatusInternalServerError, globalSnapshotSettingsResponse)
		return
	}

	if err := kotssnapshot.Redact(store); err != nil {
		logger.Error(err)
		globalSnapshotSettingsResponse.Error = "failed to redact"
		JSON(w, http.StatusInternalServerError, globalSnapshotSettingsResponse)
		return
	}

	if store.NFS != nil {
		nfsConfig, err := kotssnapshot.GetCurrentNFSConfig(r.Context(), os.Getenv("POD_NAMESPACE"))
		if err != nil {
			logger.Error(err)
			globalSnapshotSettingsResponse.Error = "failed to get nfs config"
			JSON(w, http.StatusInternalServerError, globalSnapshotSettingsResponse)
			return
		}
		globalSnapshotSettingsResponse.NFSConfig = nfsConfig
	}

	globalSnapshotSettingsResponse.Store = store
	globalSnapshotSettingsResponse.Success = true

	JSON(w, http.StatusOK, globalSnapshotSettingsResponse)
}

func (h *Handler) GetSnapshotConfig(w http.ResponseWriter, r *http.Request) {
	appSlug := mux.Vars(r)["appSlug"]
	foundApp, err := store.GetStore().GetAppFromSlug(appSlug)
	if err != nil {
		logger.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	ttl := &snapshottypes.SnapshotTTL{}
	if foundApp.SnapshotTTL != "" {
		parsedTTL, err := snapshot.ParseTTL(foundApp.SnapshotTTL)
		if err != nil {
			logger.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		ttl.InputValue = strconv.FormatInt(parsedTTL.Quantity, 10)
		ttl.InputTimeUnit = parsedTTL.Unit
		ttl.Converted = foundApp.SnapshotTTL
	} else {
		ttl.InputValue = "1"
		ttl.InputTimeUnit = "month"
		ttl.Converted = "720h"
	}

	snapshotSchedule := &snapshottypes.SnapshotSchedule{}
	if foundApp.SnapshotSchedule != "" {
		snapshotSchedule.Schedule = foundApp.SnapshotSchedule
	} else {
		snapshotSchedule.Schedule = "0 0 * * MON"
	}

	getSnapshotConfigResponse := SnapshotConfig{}
	getSnapshotConfigResponse.AutoEnabled = foundApp.SnapshotSchedule != ""
	getSnapshotConfigResponse.AutoSchedule = snapshotSchedule
	getSnapshotConfigResponse.TTl = ttl

	JSON(w, http.StatusOK, getSnapshotConfigResponse)
}

func (h *Handler) GetVeleroStatus(w http.ResponseWriter, r *http.Request) {
	getVeleroStatusResponse := VeleroStatus{}

	detectVelero, err := kotssnapshot.DetectVelero()
	if err != nil {
		logger.Error(err)
		getVeleroStatusResponse.IsVeleroInstalled = false
		JSON(w, 500, getVeleroStatusResponse)
		return
	}

	if detectVelero == nil {
		getVeleroStatusResponse.IsVeleroInstalled = false
		JSON(w, 200, getVeleroStatusResponse)
		return
	}

	getVeleroStatusResponse.IsVeleroInstalled = true
	JSON(w, 200, getVeleroStatusResponse)
}

type SaveSnapshotConfigRequest struct {
	AppID         string `json:"appId"`
	InputValue    string `json:"inputValue"`
	InputTimeUnit string `json:"inputTimeUnit"`
	Schedule      string `json:"schedule"`
	AutoEnabled   bool   `json:"autoEnabled"`
}

type SaveSnapshotConfigResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

func (h *Handler) SaveSnapshotConfig(w http.ResponseWriter, r *http.Request) {
	responseBody := SaveSnapshotConfigResponse{}
	requestBody := SaveSnapshotConfigRequest{}
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		logger.Error(err)
		responseBody.Error = "failed to decode request body"
		JSON(w, http.StatusBadRequest, responseBody)
		return
	}

	app, err := store.GetStore().GetApp(requestBody.AppID)
	if err != nil {
		logger.Error(err)
		responseBody.Error = "Failed to get app"
		JSON(w, http.StatusInternalServerError, responseBody)
		return
	}

	retention, err := snapshot.FormatTTL(requestBody.InputValue, requestBody.InputTimeUnit)
	if err != nil {
		logger.Error(err)
		responseBody.Error = fmt.Sprintf("Invalid snapshot retention: %s %s", requestBody.InputValue, requestBody.InputTimeUnit)
		JSON(w, http.StatusBadRequest, responseBody)
		return
	}

	if app.SnapshotTTL != retention {
		app.SnapshotTTL = retention
		if err := store.GetStore().SetSnapshotTTL(app.ID, retention); err != nil {
			logger.Error(err)
			responseBody.Error = "Failed to set snapshot retention"
			JSON(w, http.StatusInternalServerError, responseBody)
			return
		}
	}

	if !requestBody.AutoEnabled {
		if err := store.GetStore().SetSnapshotSchedule(app.ID, ""); err != nil {
			logger.Error(err)
			responseBody.Error = "Failed to clear snapshot schedule"
			JSON(w, http.StatusInternalServerError, responseBody)
			return
		}
		if err := store.GetStore().DeletePendingScheduledSnapshots(app.ID); err != nil {
			logger.Error(err)
			responseBody.Error = "Failed to delete scheduled snapshots"
			JSON(w, http.StatusInternalServerError, responseBody)
			return
		}
		responseBody.Success = true
		JSON(w, 200, responseBody)
		return
	}

	cronSchedule, err := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor).Parse(requestBody.Schedule)
	if err != nil {
		logger.Error(err)
		responseBody.Error = fmt.Sprintf("Invalid cron schedule expression: %s", requestBody.Schedule)
		JSON(w, http.StatusBadRequest, responseBody)
		return
	}

	if requestBody.Schedule != app.SnapshotSchedule {
		if err := store.GetStore().DeletePendingScheduledSnapshots(app.ID); err != nil {
			logger.Error(err)
			responseBody.Error = "Failed to delete scheduled snapshots"
			JSON(w, http.StatusInternalServerError, responseBody)
			return
		}
		if err := store.GetStore().SetSnapshotSchedule(app.ID, requestBody.Schedule); err != nil {
			logger.Error(err)
			responseBody.Error = "Failed to save snapshot schedule"
			JSON(w, http.StatusInternalServerError, responseBody)
			return
		}
		queued := cronSchedule.Next(time.Now())
		id := strings.ToLower(rand.String(32))
		if err := store.GetStore().CreateScheduledSnapshot(id, app.ID, queued); err != nil {
			logger.Error(err)
			responseBody.Error = "Failed to create first scheduled snapshot"
			JSON(w, http.StatusInternalServerError, responseBody)
			return
		}
	}

	responseBody.Success = true
	JSON(w, 200, responseBody)
}

type InstanceSnapshotConfig struct {
	AutoEnabled  bool                            `json:"autoEnabled"`
	AutoSchedule *snapshottypes.SnapshotSchedule `json:"autoSchedule"`
	TTl          *snapshottypes.SnapshotTTL      `json:"ttl"`
}

func (h *Handler) GetInstanceSnapshotConfig(w http.ResponseWriter, r *http.Request) {
	clusters, err := store.GetStore().ListClusters()
	if err != nil {
		logger.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if len(clusters) == 0 {
		logger.Error(errors.New("No clusters found"))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	c := clusters[0]

	ttl := &snapshottypes.SnapshotTTL{}
	if c.SnapshotTTL != "" {
		parsedTTL, err := snapshot.ParseTTL(c.SnapshotTTL)
		if err != nil {
			logger.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		ttl.InputValue = strconv.FormatInt(parsedTTL.Quantity, 10)
		ttl.InputTimeUnit = parsedTTL.Unit
		ttl.Converted = c.SnapshotTTL
	} else {
		ttl.InputValue = "1"
		ttl.InputTimeUnit = "month"
		ttl.Converted = "720h"
	}

	snapshotSchedule := &snapshottypes.SnapshotSchedule{}
	if c.SnapshotSchedule != "" {
		snapshotSchedule.Schedule = c.SnapshotSchedule
	} else {
		snapshotSchedule.Schedule = "0 0 * * MON"
	}

	getInstanceSnapshotConfigResponse := InstanceSnapshotConfig{}
	getInstanceSnapshotConfigResponse.AutoEnabled = c.SnapshotSchedule != ""
	getInstanceSnapshotConfigResponse.AutoSchedule = snapshotSchedule
	getInstanceSnapshotConfigResponse.TTl = ttl

	JSON(w, http.StatusOK, getInstanceSnapshotConfigResponse)
}

type SaveInstanceSnapshotConfigRequest struct {
	InputValue    string `json:"inputValue"`
	InputTimeUnit string `json:"inputTimeUnit"`
	Schedule      string `json:"schedule"`
	AutoEnabled   bool   `json:"autoEnabled"`
}

type SaveInstanceSnapshotConfigResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

func (h *Handler) SaveInstanceSnapshotConfig(w http.ResponseWriter, r *http.Request) {
	responseBody := SaveInstanceSnapshotConfigResponse{}
	requestBody := SaveInstanceSnapshotConfigRequest{}
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		logger.Error(err)
		responseBody.Error = "failed to decode request body"
		JSON(w, http.StatusBadRequest, responseBody)
		return
	}

	clusters, err := store.GetStore().ListClusters()
	if err != nil {
		logger.Error(err)
		responseBody.Error = "Failed to list clusters"
		JSON(w, http.StatusInternalServerError, responseBody)
		return
	}
	if len(clusters) == 0 {
		err := errors.New("No clusters found")
		logger.Error(err)
		responseBody.Error = err.Error()
		JSON(w, http.StatusInternalServerError, responseBody)
		return
	}
	c := clusters[0]

	retention, err := snapshot.FormatTTL(requestBody.InputValue, requestBody.InputTimeUnit)
	if err != nil {
		logger.Error(err)
		responseBody.Error = fmt.Sprintf("Invalid instance snapshot retention: %s %s", requestBody.InputValue, requestBody.InputTimeUnit)
		JSON(w, http.StatusBadRequest, responseBody)
		return
	}

	if c.SnapshotTTL != retention {
		c.SnapshotTTL = retention
		if err := store.GetStore().SetInstanceSnapshotTTL(c.ClusterID, retention); err != nil {
			logger.Error(err)
			responseBody.Error = "Failed to set instance snapshot retention"
			JSON(w, http.StatusInternalServerError, responseBody)
			return
		}
	}

	if !requestBody.AutoEnabled {
		if err := store.GetStore().SetInstanceSnapshotSchedule(c.ClusterID, ""); err != nil {
			logger.Error(err)
			responseBody.Error = "Failed to clear instance snapshot schedule"
			JSON(w, http.StatusInternalServerError, responseBody)
			return
		}
		if err := store.GetStore().DeletePendingScheduledInstanceSnapshots(c.ClusterID); err != nil {
			logger.Error(err)
			responseBody.Error = "Failed to delete pending scheduled instance snapshots"
			JSON(w, http.StatusInternalServerError, responseBody)
			return
		}
		responseBody.Success = true
		JSON(w, 200, responseBody)
		return
	}

	cronSchedule, err := cron.ParseStandard(requestBody.Schedule)
	if err != nil {
		logger.Error(err)
		responseBody.Error = fmt.Sprintf("Invalid cron schedule expression: %s", requestBody.Schedule)
		JSON(w, http.StatusBadRequest, responseBody)
		return
	}

	if requestBody.Schedule != c.SnapshotSchedule {
		if err := store.GetStore().DeletePendingScheduledInstanceSnapshots(c.ClusterID); err != nil {
			logger.Error(err)
			responseBody.Error = "Failed to delete scheduled snapshots"
			JSON(w, http.StatusInternalServerError, responseBody)
			return
		}
		if err := store.GetStore().SetInstanceSnapshotSchedule(c.ClusterID, requestBody.Schedule); err != nil {
			logger.Error(err)
			responseBody.Error = "Failed to save instance snapshot schedule"
			JSON(w, http.StatusInternalServerError, responseBody)
			return
		}
		queued := cronSchedule.Next(time.Now())
		id := strings.ToLower(rand.String(32))
		if err := store.GetStore().CreateScheduledInstanceSnapshot(id, c.ClusterID, queued); err != nil {
			logger.Error(err)
			responseBody.Error = "Failed to create first scheduled instance snapshot"
			JSON(w, http.StatusInternalServerError, responseBody)
			return
		}
	}

	responseBody.Success = true
	JSON(w, http.StatusOK, responseBody)
}
