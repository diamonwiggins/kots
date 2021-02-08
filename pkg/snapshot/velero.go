package snapshot

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/pkg/errors"
	"github.com/replicatedhq/kots/pkg/k8sutil"
	"github.com/replicatedhq/kots/pkg/kotsadm"
	kotsadmtypes "github.com/replicatedhq/kots/pkg/kotsadm/types"
	kotsadmversion "github.com/replicatedhq/kots/pkg/kotsadm/version"
	"github.com/replicatedhq/kots/pkg/kotsutil"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/client"
	veleroclientv1 "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned/typed/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/install"
	kubeutil "github.com/vmware-tanzu/velero/pkg/util/kube"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kuberneteserrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

var (
	dockerImageNameRegex = regexp.MustCompile("(?:([^\\/]+)\\/)?(?:([^\\/]+)\\/)?([^@:\\/]+)(?:[@:](.+))")
)

const (
	veleroNamespace                       = velerov1api.DefaultNamespace
	veleroDeploymentName                  = "velero"
	defaultVeleroImage                    = "velero/velero:v1.5.1"
	defaultVeleroAWSPluginImage           = "velero/velero-plugin-for-aws:v1.1.0"
	defaultVeleroResticRestoreHelperImage = "velero/velero-restic-restore-helper:v1.5.1"
	resticConfigMapName                   = "restic-restore-action-config"
	resticDaemonSetName                   = "restic"
)

type VeleroInstallOptions struct {
	ProviderName         string
	BucketName           string
	Prefix               string
	SecretData           []byte
	BackupStorageConfig  map[string]string
	VolumeSnapshotConfig map[string]string
	Wait                 bool
}

type VeleroStatus struct {
	Version string
	Plugins []string
	Status  string

	ResticVersion string
	ResticStatus  string
}

func InstallVelero(ctx context.Context, clientset kubernetes.Interface, veleroInstallOptions VeleroInstallOptions, kotsadmNamespace string, kotsadmRegistryOptions kotsadmtypes.KotsadmOptions) error {
	veleroImage, awsPluginImage, _, _, err := rewriteVeleroImages(ctx, clientset, kotsadmNamespace, kotsadmRegistryOptions)
	if err != nil {
		return errors.Wrap(err, "failed to rewrite images")
	}

	veleroPodResources, err := kubeutil.ParseResourceRequirements(install.DefaultVeleroPodCPURequest, install.DefaultVeleroPodMemRequest, install.DefaultVeleroPodCPULimit, install.DefaultVeleroPodMemLimit)
	if err != nil {
		return errors.Wrap(err, "failed to parse velero resource requirements")
	}
	resticPodResources, err := kubeutil.ParseResourceRequirements(install.DefaultResticPodCPURequest, install.DefaultResticPodMemRequest, install.DefaultResticPodCPULimit, install.DefaultResticPodMemLimit)
	if err != nil {
		return errors.Wrap(err, "failed to parse restic resource requirements")
	}

	vo := &install.VeleroOptions{
		Namespace:               veleroNamespace,
		Image:                   veleroImage,
		ProviderName:            veleroInstallOptions.ProviderName,
		Bucket:                  veleroInstallOptions.BucketName,
		Prefix:                  veleroInstallOptions.Prefix,
		VeleroPodResources:      veleroPodResources,
		ResticPodResources:      resticPodResources,
		SecretData:              veleroInstallOptions.SecretData,
		UseRestic:               true,
		UseVolumeSnapshots:      true,
		BSLConfig:               veleroInstallOptions.BackupStorageConfig,
		VSLConfig:               veleroInstallOptions.VolumeSnapshotConfig,
		Plugins:                 []string{awsPluginImage},
		NoDefaultBackupLocation: false,
		DefaultVolumesToRestic:  true,
	}

	resources, err := install.AllResources(vo)
	if err != nil {
		return errors.Wrap(err, "failed to get resources")
	}

	factory, err := getVeleroFactory()
	if err != nil {
		return errors.Wrap(err, "failed to get velero factory")
	}

	errorMsg := fmt.Sprintf("\n\nError installing Velero. Use `kubectl logs deploy/velero -n %s` to check the deploy logs", veleroNamespace)

	err = install.Install(*factory, resources, os.Stdout)
	if err != nil {
		return errors.Wrap(err, errorMsg)
	}

	if err := ConfigureVeleroImages(ctx, clientset, kotsadmNamespace, kotsadmRegistryOptions); err != nil {
		return errors.Wrap(err, "failed to configure velero images")
	}

	if veleroInstallOptions.Wait {
		fmt.Println("Waiting for Velero deployment and restic daemonset to be ready.")
		if err := WaitForVeleroReady(ctx, clientset, factory); err != nil {
			return errors.Wrap(err, errorMsg)
		}
	}

	return nil
}

// ConfigureVeleroImages will rewrite velero/restic images based on the provided kotsadm registry options and will also add/remove imagePullSecrets if necessary
// no-op on kurl (embedded) clusters, because we don't rewrite images in kurl
func ConfigureVeleroImages(ctx context.Context, clientset kubernetes.Interface, kotsadmNamespace string, kotsadmRegistryOptions kotsadmtypes.KotsadmOptions) error {
	if kotsutil.IsKurl(clientset) && kotsadmNamespace == metav1.NamespaceDefault {
		return nil
	}

	veleroImage, awsPluginImage, resticRestoreHelperImage, imagePullSecrets, err := rewriteVeleroImages(ctx, clientset, kotsadmNamespace, kotsadmRegistryOptions)
	if err != nil {
		return errors.Wrap(err, "failed to rewrite images")
	}

	if err := kotsadm.EnsurePrivateKotsadmRegistrySecret(veleroNamespace, kotsadmRegistryOptions, clientset); err != nil {
		return errors.Wrap(err, "failed to ensure private kotsadm registry secret")
	}

	veleroDeployment, err := clientset.AppsV1().Deployments(veleroNamespace).Get(ctx, veleroDeploymentName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to get velero deployment")
	}

	veleroDeployment.Spec.Template.Spec.ImagePullSecrets = imagePullSecrets
	veleroDeployment.Spec.Template.Spec.InitContainers[0].Image = awsPluginImage
	veleroDeployment.Spec.Template.Spec.Containers[0].Image = veleroImage

	_, err = clientset.AppsV1().Deployments(veleroNamespace).Update(ctx, veleroDeployment, metav1.UpdateOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to update velero deployment")
	}

	// configure restic, ref: https://velero.io/docs/v1.5/restic/#configure-restic-daemonset-spec
	// check if image has been rewritten
	if resticRestoreHelperImage != defaultVeleroResticRestoreHelperImage {
		// create/update restic configmap to rewrite the velero-restic-restore-helper image
		resticConfigMap, err := clientset.CoreV1().ConfigMaps(veleroNamespace).Get(ctx, resticConfigMapName, metav1.GetOptions{})
		if err != nil && !kuberneteserrors.IsNotFound(err) {
			return errors.Wrap(err, "failed to get restic configmap")
		}
		if kuberneteserrors.IsNotFound(err) {
			resticConfigMap = resticConfigMapResource(resticRestoreHelperImage)
			_, err := clientset.CoreV1().ConfigMaps(veleroNamespace).Create(ctx, resticConfigMap, metav1.CreateOptions{})
			if err != nil {
				return errors.Wrap(err, "failed to create restic configmap")
			}
		} else {
			if resticConfigMap.Data == nil {
				resticConfigMap.Data = map[string]string{}
			}
			resticConfigMap.Data["image"] = resticRestoreHelperImage
			_, err := clientset.CoreV1().ConfigMaps(veleroNamespace).Update(ctx, resticConfigMap, metav1.UpdateOptions{})
			if err != nil {
				return errors.Wrap(err, "failed to update restic configmap")
			}
		}
	} else {
		err := clientset.CoreV1().ConfigMaps(veleroNamespace).Delete(ctx, resticConfigMapName, metav1.DeleteOptions{})
		if err != nil && !kuberneteserrors.IsNotFound(err) {
			return errors.Wrap(err, "failed to delete restic configmap")
		}
	}

	resticDaemonSet, err := clientset.AppsV1().DaemonSets(veleroNamespace).Get(ctx, resticDaemonSetName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to get restic daemonset")
	}

	resticDaemonSet.Spec.Template.Spec.ImagePullSecrets = imagePullSecrets
	resticDaemonSet.Spec.Template.Spec.Containers[0].Image = veleroImage

	_, err = clientset.AppsV1().DaemonSets(veleroNamespace).Update(ctx, resticDaemonSet, metav1.UpdateOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to update restic daemonset")
	}

	return nil
}

func resticConfigMapResource(image string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: resticConfigMapName,
			Labels: map[string]string{
				"velero.io/plugin-config": "",
				"velero.io/restic":        "RestoreItemAction",
			},
		},
		Data: map[string]string{
			"image": image,
		},
	}
}

func WaitForVeleroReady(ctx context.Context, clientset kubernetes.Interface, factory *client.DynamicFactory) error {
	if factory == nil {
		var err error
		factory, err = getVeleroFactory()
		if err != nil {
			return errors.Wrap(err, "failed to get velero factory")
		}
	}
	if err := waitForVeleroDeploymentReady(ctx, clientset, *factory); err != nil {
		return errors.Wrap(err, "failed to wait for velero deployment to be ready")
	}
	if _, err := install.DaemonSetIsReady(*factory, veleroNamespace); err != nil {
		return errors.Wrap(err, "failed to wait for Velero restic daemonset to be ready.")
	}
	return nil
}

func waitForVeleroDeploymentReady(ctx context.Context, clientset kubernetes.Interface, factory client.DynamicFactory) error {
	// both of these functions are needed to check if velero deployment is ready
	if err := k8sutil.WaitForDeploymentReady(ctx, clientset, veleroNamespace, veleroDeploymentName, time.Minute*2); err != nil {
		return err
	}
	if _, err := install.DeploymentIsReady(factory, veleroNamespace); err != nil {
		return err
	}
	return nil
}

func getVeleroFactory() (*client.DynamicFactory, error) {
	config := client.VeleroConfig{}
	f := client.NewFactory("install", config)

	dynamicClient, err := f.DynamicClient()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get dynamic client")
	}
	factory := client.NewDynamicFactory(dynamicClient)

	return &factory, nil
}

func rewriteVeleroImages(ctx context.Context, clientset kubernetes.Interface, kotsadmNamespace string, kotsadmRegistryOptions kotsadmtypes.KotsadmOptions) (veleroImage string, awsPluginImage string, resticRestoreHelperImage string, imagePullSecrets []corev1.LocalObjectReference, finalErr error) {
	veleroImage = defaultVeleroImage
	awsPluginImage = defaultVeleroAWSPluginImage
	resticRestoreHelperImage = defaultVeleroResticRestoreHelperImage
	imagePullSecrets = []corev1.LocalObjectReference{}

	if !kotsutil.IsKurl(clientset) || kotsadmNamespace != metav1.NamespaceDefault {
		var err error
		imageRewriteFn := kotsadmversion.ImageRewriteKotsadmRegistry(kotsadmNamespace, &kotsadmRegistryOptions)
		veleroImage, imagePullSecrets, err = imageRewriteFn(veleroImage, false)
		if err != nil {
			finalErr = errors.Wrap(err, "failed to rewrite velero image")
			return
		}
		awsPluginImage, _, err = imageRewriteFn(awsPluginImage, false)
		if err != nil {
			finalErr = errors.Wrap(err, "failed to rewrite aws plugin image")
			return
		}
		resticRestoreHelperImage, _, err = imageRewriteFn(resticRestoreHelperImage, false)
		if err != nil {
			finalErr = errors.Wrap(err, "failed to rewrite restic restore helper image")
			return
		}
	}

	return
}

func InstallVeleroFromStoreInternal(ctx context.Context, clientset kubernetes.Interface, kotsadmNamespace string, kotsadmRegistryOptions kotsadmtypes.KotsadmOptions, wait bool) error {
	storeInternal, bucketName, err := buildStoreInternal(ctx, clientset, kotsadmNamespace)
	if err != nil {
		return errors.Wrap(err, "failed to build internal store")
	}

	err = validateInternal(storeInternal, bucketName, kotsadmNamespace)
	if err != nil {
		return errors.Wrap(err, "failed to validate internal store")
	}

	creds, err := buildAWSCredentials(storeInternal.AccessKeyID, storeInternal.SecretAccessKey)
	if err != nil {
		return errors.Wrap(err, "failed to format credentials")
	}

	veleroInstallOptions := VeleroInstallOptions{
		ProviderName: "aws",
		BucketName:   bucketName,
		SecretData:   creds,
		BackupStorageConfig: map[string]string{
			"region":           storeInternal.Region,
			"s3ForcePathStyle": "true",
			"s3Url":            storeInternal.Endpoint,
			"publicUrl":        getStoreInternalPublicURL(clientset, storeInternal, kotsadmNamespace),
		},
		VolumeSnapshotConfig: map[string]string{
			"region": storeInternal.Region,
		},
		Wait: wait,
	}

	return InstallVelero(ctx, clientset, veleroInstallOptions, kotsadmNamespace, kotsadmRegistryOptions)
}

func InstallVeleroFromStoreNFS(ctx context.Context, clientset kubernetes.Interface, kotsadmNamespace string, kotsadmRegistryOptions kotsadmtypes.KotsadmOptions, wait bool) error {
	storeNFS, err := buildStoreNFS(ctx, clientset, kotsadmNamespace)
	if err != nil {
		return errors.Wrap(err, "failed to build nfs store")
	}

	err = validateNFS(storeNFS, NFSMinioBucketName)
	if err != nil {
		return errors.Wrap(err, "failed to validate nfs store")
	}

	nfsCreds, err := buildAWSCredentials(storeNFS.AccessKeyID, storeNFS.SecretAccessKey)
	if err != nil {
		return errors.Wrap(err, "failed to format credentials")
	}

	veleroInstallOptions := VeleroInstallOptions{
		ProviderName: NFSMinioProvider,
		BucketName:   NFSMinioBucketName,
		SecretData:   nfsCreds,
		BackupStorageConfig: map[string]string{
			"region":           storeNFS.Region,
			"s3ForcePathStyle": "true",
			"s3Url":            storeNFS.Endpoint,
			"publicUrl":        getStoreNFSPublicURL(storeNFS),
		},
		VolumeSnapshotConfig: map[string]string{
			"region": storeNFS.Region,
		},
		Wait: wait,
	}

	return InstallVelero(ctx, clientset, veleroInstallOptions, kotsadmNamespace, kotsadmRegistryOptions)
}

func DetectVeleroNamespace() (string, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return "", errors.Wrap(err, "failed to get cluster config")
	}

	veleroClient, err := veleroclientv1.NewForConfig(cfg)
	if err != nil {
		return "", errors.Wrap(err, "failed to create velero clientset")
	}

	backupStorageLocations, err := veleroClient.BackupStorageLocations("").List(context.TODO(), metav1.ListOptions{})
	if kuberneteserrors.IsNotFound(err) {
		return "", nil
	}

	if err != nil {
		// can't detect velero
		return "", nil
	}

	for _, backupStorageLocation := range backupStorageLocations.Items {
		if backupStorageLocation.Name == "default" {
			return backupStorageLocation.Namespace, nil
		}
	}

	return "", nil
}

func DetectVelero() (*VeleroStatus, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get cluster config")
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create clientset")
	}

	veleroNamespace, err := DetectVeleroNamespace()
	if err != nil {
		return nil, errors.Wrap(err, "failed to detect velero namespace")
	}

	if veleroNamespace == "" {
		return nil, nil
	}

	veleroStatus := VeleroStatus{
		Plugins: []string{},
	}

	possibleDeployments, err := listPossibleVeleroDeployments(clientset, veleroNamespace)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list possible velero deployments")
	}

	for _, deployment := range possibleDeployments {
		for _, initContainer := range deployment.Spec.Template.Spec.InitContainers {
			// the default installation is to name these like "velero-plugin-for-aws"
			veleroStatus.Plugins = append(veleroStatus.Plugins, initContainer.Name)
		}

		matches := dockerImageNameRegex.FindStringSubmatch(deployment.Spec.Template.Spec.Containers[0].Image)
		if len(matches) == 5 {
			status := "NotReady"

			if deployment.Status.AvailableReplicas > 0 {
				status = "Ready"
			}

			veleroStatus.Version = matches[4]
			veleroStatus.Status = status

			goto DeploymentFound
		}
	}
DeploymentFound:

	daemonsets, err := listPossibleResticDaemonsets(clientset, veleroNamespace)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list restic daemonsets")
	}
	for _, daemonset := range daemonsets {
		matches := dockerImageNameRegex.FindStringSubmatch(daemonset.Spec.Template.Spec.Containers[0].Image)
		if len(matches) == 5 {
			status := "NotReady"

			if daemonset.Status.NumberAvailable > 0 {
				if daemonset.Status.NumberUnavailable == 0 {
					status = "Ready"
				}
			}

			veleroStatus.ResticVersion = matches[4]
			veleroStatus.ResticStatus = status

			goto ResticFound
		}
	}
ResticFound:

	return &veleroStatus, nil
}

// listPossibleVeleroDeployments filters with a label selector based on how we've found velero deployed
// using the CLI or the Helm Chart.
func listPossibleVeleroDeployments(clientset *kubernetes.Clientset, namespace string) ([]v1.Deployment, error) {
	deployments, err := clientset.AppsV1().Deployments(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "component=velero",
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list deployments")
	}

	helmDeployments, err := clientset.AppsV1().Deployments(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=velero",
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list helm deployments")
	}

	return append(deployments.Items, helmDeployments.Items...), nil
}

// listPossibleResticDaemonsets filters with a label selector based on how we've found restic deployed
// using the CLI or the Helm Chart.
func listPossibleResticDaemonsets(clientset *kubernetes.Clientset, namespace string) ([]v1.DaemonSet, error) {
	daemonsets, err := clientset.AppsV1().DaemonSets(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "component=velero",
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list daemonsets")
	}

	helmDaemonsets, err := clientset.AppsV1().DaemonSets(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=velero",
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list helm daemonsets")
	}

	return append(daemonsets.Items, helmDaemonsets.Items...), nil
}

// restartVelero will restart velero (and restic)
func restartVelero() error {
	cfg, err := config.GetConfig()
	if err != nil {
		return errors.Wrap(err, "failed to get cluster config")
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return errors.Wrap(err, "failed to create clientset")
	}

	namespace, err := DetectVeleroNamespace()
	if err != nil {
		return errors.Wrap(err, "failed to detect velero namespace")
	}

	veleroDeployments, err := listPossibleVeleroDeployments(clientset, namespace)
	if err != nil {
		return errors.Wrap(err, "failed to list velero deployments")
	}

	for _, veleroDeployment := range veleroDeployments {
		pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labels.SelectorFromSet(veleroDeployment.Labels).String(),
		})
		if err != nil {
			return errors.Wrap(err, "failed to list pods in velero deployment")
		}

		for _, pod := range pods.Items {
			if err := clientset.CoreV1().Pods(namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{}); err != nil {
				return errors.Wrap(err, "failed to delete velero deployment")
			}

		}
	}

	resticDaemonSets, err := listPossibleResticDaemonsets(clientset, namespace)
	if err != nil {
		return errors.Wrap(err, "failed to list restic daemonsets")
	}

	for _, resticDaemonSet := range resticDaemonSets {
		pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labels.SelectorFromSet(resticDaemonSet.Labels).String(),
		})
		if err != nil {
			return errors.Wrap(err, "failed to list pods in restic daemonset")
		}

		for _, pod := range pods.Items {
			if err := clientset.CoreV1().Pods(namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{}); err != nil {
				return errors.Wrap(err, "failed to delete restic daemonset")
			}

		}
	}

	return nil
}
