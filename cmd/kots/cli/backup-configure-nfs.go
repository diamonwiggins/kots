package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"

	"github.com/manifoldco/promptui"
	"github.com/pkg/errors"
	"github.com/replicatedhq/kots/pkg/k8sutil"
	"github.com/replicatedhq/kots/pkg/logger"
	"github.com/replicatedhq/kots/pkg/snapshot"
	"github.com/replicatedhq/kots/pkg/snapshot/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
				Wait: true,
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

			veleroNamespace, err := snapshot.DetectVeleroNamespace()
			if err != nil {
				return errors.Wrap(err, "failed to detect velero namespace")
			}

			if veleroNamespace == "" {
				// velero not found, install and configure velero

				log.ActionWithoutSpinner("Installing and configuring Velero")

				nfsStore, err := snapshot.BuildNFSStore(cmd.Context(), clientset, namespace)
				if err != nil {
					return errors.Wrap(err, "failed to build nfs store")
				}

				if err := snapshot.InstallVeleroFromNFSStore(cmd.Context(), clientset, nfsStore, namespace, *registryOptions, v.GetBool("wait-for-velero")); err != nil {
					return errors.Wrap(err, "failed to install velero")
				}

				log.ActionWithoutSpinner("NFS configured successfully.")

				return nil
			}

			log.ActionWithSpinner("Configuring Velero")

			err = snapshot.ConfigureVeleroDeployment(cmd.Context(), clientset, namespace, *registryOptions)
			if err != nil {
				log.FinishSpinnerWithError()
				return errors.Wrap(err, "failed to configure velero deployment")
			}

			configureStoreOptions := snapshot.ConfigureStoreOptions{
				NFS:              true,
				KotsadmNamespace: namespace,
			}
			_, err = snapshot.ConfigureStore(configureStoreOptions)
			if err != nil {
				log.FinishSpinnerWithError()
				return errors.Wrap(err, "failed to configure store")
			}

			log.FinishSpinner()

			if v.GetBool("wait-for-velero") {
				log.ActionWithSpinner("Waiting for Velero to be ready")

				err := snapshot.WaitForVeleroReady(cmd.Context(), clientset, nil)
				if err != nil {
					log.FinishSpinnerWithError()
					return errors.Wrap(err, "failed to wait for velero")
				}

				log.FinishSpinner()
			}

			log.ActionWithoutSpinner("NFS configured successfully.")

			return nil
		},
	}

	cmd.Flags().String("path", "", "path that is exported by the NFS server")
	cmd.Flags().String("server", "", "the hostname or IP address of the NFS server")
	cmd.Flags().StringP("namespace", "n", "", "the namespace in which kots/kotsadm is installed")
	cmd.Flags().Bool("wait-for-velero", true, "wait for Velero to be ready")
	cmd.Flags().Bool("airgap", false, "set to true to run in airgapped mode")

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
