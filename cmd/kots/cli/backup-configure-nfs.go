package cli

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"

	"github.com/manifoldco/promptui"
	"github.com/pkg/errors"
	"github.com/replicatedhq/kots/pkg/k8sutil"
	"github.com/replicatedhq/kots/pkg/logger"
	"github.com/replicatedhq/kots/pkg/snapshot"
	"github.com/replicatedhq/kots/pkg/snapshot/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/ini.v1"
)

func BackupConfigureNFSCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "configure-nfs",
		Short:         "Configure snapshots to use NFS as storage",
		Long:          ``,
		SilenceUsage:  true,
		SilenceErrors: false,
		PreRun: func(cmd *cobra.Command, args []string) {
			viper.BindPFlags(cmd.Flags())
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			v := viper.GetViper()

			namespace := v.GetString("namespace")
			if err := validateNamespace(namespace); err != nil {
				return err
			}

			nfsPath := v.GetString("path")
			if nfsPath == "" {
				return errors.New("path is rquired")
			}

			nfsServer := v.GetString("server")
			if nfsServer == "" {
				return errors.New("server is rquired")
			}

			clientset, err := k8sutil.GetClientset(kubernetesConfigFlags)
			if err != nil {
				return errors.Wrap(err, "failed to get clientset")
			}

			registryOptions, err := getRegistryConfig(v)
			if err != nil {
				return errors.Wrap(err, "failed to get registry config")
			}

			log := logger.NewLogger()
			log.ActionWithSpinner("Setting up NFS Minio")

			deployOptions := snapshot.NFSDeployOptions{
				Namespace:   namespace,
				IsOpenShift: k8sutil.IsOpenShift(clientset),
				NFSConfig: types.NFSConfig{
					Path:   nfsPath,
					Server: nfsServer,
				},
			}
			if err := snapshot.DeployNFSMinio(cmd.Context(), clientset, deployOptions, *registryOptions); err != nil {
				if _, ok := errors.Cause(err).(*snapshot.ResetNFSError); ok {
					log.FinishSpinnerWithError()
					forceReset := promptForNFSReset(log, err.Error())
					if forceReset {
						log.ActionWithSpinner("Re-configuring NFS Minio")
						deployOptions.ForceReset = true
						if err := snapshot.DeployNFSMinio(cmd.Context(), clientset, deployOptions, *registryOptions); err != nil {
							log.FinishSpinnerWithError()
							return errors.Wrap(err, "failed to force deploy nfs minio")
						}
					}
				} else {
					log.FinishSpinnerWithError()
					return errors.Wrap(err, "failed to deploy nfs minio")
				}
			}

			log.FinishSpinner()
			log.ActionWithSpinner("Waiting for NFS Minio to be ready")

			err = snapshot.WaitForNFSMinioReady(cmd.Context(), clientset, namespace, time.Minute*5)
			if err != nil {
				log.FinishSpinnerWithError()
				return errors.Wrap(err, "failed to wait for nfs minio")
			}

			log.FinishSpinner()
			log.ActionWithSpinner("Creating default bucket")

			err = snapshot.CreateNFSBucket(cmd.Context(), clientset, namespace)
			if err != nil {
				log.FinishSpinnerWithError()
				return errors.Wrap(err, "failed to create default bucket")
			}

			log.FinishSpinner()

			veleroNamespace, err := snapshot.DetectVeleroNamespace()
			if err != nil {
				return errors.Wrap(err, "failed to detect velero namespace")
			}

			if veleroNamespace == "" {
				// velero not found, install and configure velero

				log.ActionWithoutSpinner("Installing and configuring Velero")

				nfsStore, err := snapshot.BuildNFSStore(clientset, namespace)
				if err != nil {
					return errors.Wrap(err, "failed to build nfs store")
				}

				creds, err := formatCredentials(nfsStore.AccessKeyID, nfsStore.SecretAccessKey)
				if err != nil {
					return errors.Wrap(err, "failed to format credentials")
				}

				publicURL := fmt.Sprintf("http://%s:%d", nfsStore.ObjectStoreClusterIP, snapshot.NFSMinioServicePort)

				opts := snapshot.VeleroInstallOptions{
					Plugins:      []string{"velero/velero-plugin-for-aws:v1.1.0"},
					ProviderName: snapshot.NFSMinioProvider,
					BucketName:   snapshot.NFSMinioBucketName,
					SecretData:   creds,
					BackupStorageConfig: map[string]string{
						"region":           snapshot.NFSMinioRegion,
						"s3ForcePathStyle": "true",
						"s3Url":            nfsStore.Endpoint,
						"publicURL":        publicURL,
					},
					VolumeSnapshotConfig: map[string]string{
						"region": snapshot.NFSMinioRegion,
					},
					Wait: true,
				}
				if err := snapshot.InstallVelero(opts, *registryOptions); err != nil {
					return errors.Wrap(err, "failed to install velero")
				}

				log.ActionWithoutSpinner("NFS configured successfully.")

				return nil
			}

			log.ActionWithSpinner("Configuring Velero")

			_, err = snapshot.GetGlobalStore(nil)
			if err != nil {
				log.FinishSpinnerWithError()
				return errors.Wrap(err, "failed to get global store")
			}

			configureStoreOptions := snapshot.ConfigureStoreOptions{
				NFS:           true,
				KOTSNamespace: namespace,
			}
			_, err = snapshot.ConfigureStore(configureStoreOptions)
			if err != nil {
				log.FinishSpinnerWithError()
				return errors.Wrap(err, "failed to configure store")
			}

			log.FinishSpinner()
			log.ActionWithoutSpinner("NFS configured successfully.")

			return nil
		},
	}

	cmd.Flags().String("path", "", "path that is exported by the NFS server")
	cmd.Flags().String("server", "", "the hostname or IP address of the NFS server")
	cmd.Flags().StringP("namespace", "n", "", "the namespace in which kots/kotsadm is installed")
	cmd.Flags().Bool("airgap", false, "set to true to run in airgapped mode.")

	registryFlags(cmd.Flags())

	return cmd
}

func promptForNFSReset(log *logger.Logger, warningMsg string) bool {
	// this is a workaround to avoid this issue: https://github.com/manifoldco/promptui/issues/122
	red := color.New(color.BgRed)
	log.ColoredInfo(fmt.Sprintf("\n%s", warningMsg), red)

	prompt := promptui.Prompt{
		Label:     "Would you like to continue",
		IsConfirm: true,
	}

	for {
		resp, err := prompt.Run()
		if err == promptui.ErrInterrupt {
			os.Exit(-1)
		}
		if strings.ToLower(resp) == "n" {
			os.Exit(-1)
		}
		if strings.ToLower(resp) == "y" {
			log.ActionWithoutSpinner("")
			return true
		}
	}
}

func formatCredentials(accessKeyID, secretAccessKey string) ([]byte, error) {
	awsCfg := ini.Empty()
	section, err := awsCfg.NewSection("default")
	if err != nil {
		return nil, errors.Wrap(err, "failed to create default section in aws creds")
	}
	_, err = section.NewKey("aws_access_key_id", accessKeyID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create access key")
	}

	_, err = section.NewKey("aws_secret_access_key", secretAccessKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create secret access key")
	}

	var awsCredentials bytes.Buffer
	writer := bufio.NewWriter(&awsCredentials)
	_, err = awsCfg.WriteTo(writer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to write ini")
	}
	if err := writer.Flush(); err != nil {
		return nil, errors.Wrap(err, "failed to flush buffer")
	}

	return awsCredentials.Bytes(), nil
}
